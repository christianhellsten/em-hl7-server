require 'eventmachine'
#require 'ruby-hl7'
require 'logger'
require 'yaml'
require 'pg'

module HL7Server
  module MLLPProtocol
    # TODO Should be configurable.
    HL7_MSG_START = /\xb/
    HL7_MSG_END = /\x1c/
    #HL7_MSG_END = /quit/

    #
    # Called by the event loop immediately after the network connection has been
    # established, and before resumption of the network loop.
    #
    def post_init
      if ssl_config
        key = ssl_config.fetch(:private_key_file, 'server.key')
        crt = ssl_config.fetch(:cert_chain_file, 'server.crt')
        verify_peer = ssl_config.fetch(:verify_peer, true)
        start_tls private_key_file: key, cert_chain_file: crt, verify_peer: verify_peer
        log.debug "#{remote_ip}: SSL connection established"
      else
        log.debug "#{remote_ip}: Connection established"
      end
      new_message
    end

    #
    # Called by the event loop whenever data has been received by the network
    # connection. It is never called by user code.
    #
    # NOTE: Depending on the protocol, buffer sizes and OS networking stack
    # configuration, incoming data may or may not be "a complete message". It
    # is up to this handler to detect content boundaries to determine whether
    # all the content (for example, full HTTP request) has been received and
    # can be processed.
    #
    # TODO: The TCP stream # could contain any part of one or many packets, or
    # none. See:
    # https://github.com/apache/camel/blob/master/components/camel-hl7/src/main/java/org/apache/camel/component/hl7/HL7MLLPDecoder.java
    #
    # This should act like a state machine when parsing data.
    #
    def receive_data(data)
      @message << data
      if end_of_message?
        process_hl7_message(@message)
      end
    rescue => e
      log.error %Q{#{e.inspect}:\n#{e.backtrace.join("\n")}}
    end

    #
    # Called by the framework whenever a connection (either a server or client
    # connection) is closed.
    #
    def unbind
      log.debug "#{remote_ip}: Connection closed"
    end

    #
    # Called when a complete HL7 message is received.
    #
    def process_hl7_message(message)
      message.strip!
      log.debug do
        delim = '*'*80
        "#{remote_ip}: Received HL7 message:\n#{delim}\n#{message}\n#{delim}"
      end
      hl7_message_received(message)
      new_message
    end

    def new_message
      @message = ''
    end

    def end_of_message?
      @message.slice!(HL7_MSG_END)
    end

    #
    # Returns SSL configuration or nil.
    #
    def ssl_config
      @ssl_config ||= Options[:ssl]
    end
  end

  module RemoteIP
    #
    # Return the IP and port of the remote client.
    #
    def remote_ip
      @remote_ip ||= begin
                       port, ip = Socket.unpack_sockaddr_in(get_peername)
                       "#{ip}:#{port}"
                     end
    end
  end

  module Logging
    def log
      @@log ||= begin
                  log = Logger.new(log_file)
                  log.level = log_level
                  log
                end
    end

    def log_file
      Options.fetch(:log).fetch(:file, 'hl7_server.log')
    end

    def log_level
      Logger.const_get(Options.fetch(:log).fetch(:level, :debug).to_s.upcase)
    end
  end

  #
  # CREATE TABLE hl7_messages (
  #   id BIGSERIAL PRIMARY KEY,
  #   message TEXT NOT NULL,
  #   type varchar(50) NOT NULL,
  #   processed_at TIMESTAMP,
  #   updated_at TIMESTAMP NOT NULL,
  #   created_at TIMESTAMP NOT NULL
  # );
  #
  class HL7Message
    def initialize(attributes = {})
      @message = attributes.fetch(:message)
      validate!
    end

    #
    # TODO
    #
    def validate!
      #HL7::Message.new(@message).to_mllp
    end

    def save!
      begin
        conn = PG::Connection.new(Options.fetch(:database))
        conn.setnonblocking(false)
        conn.set_client_encoding 'utf8'
        sql = "INSERT INTO hl7_messages (message, type, updated_at, created_at)
      VALUES($1, 'hl7', now(), now())
      RETURNING ID;"
        res = conn.exec(sql, [@message])
        res.getvalue(0, 0)
      ensure
        conn.finish if conn
      end
    end
  end

  class MLLPServer < EventMachine::Connection
    include Logging
    include MLLPProtocol
    include RemoteIP

    def hl7_message_received(message)
      id = HL7Message.new(message: message).save!
      log.info "Stored HL7 message ##{id}"
    end
  end

  class Options
    class << self
      def [](key)
        options[key.to_s]
      end

      def listen_host
        fetch(:listen).fetch('host')
      end

      def listen_port
        fetch(:listen).fetch('port')
      end

      def fetch(key)
        options.fetch(key.to_s)
      end

      def valid?
        options
      end

      private

      def options
        @options ||= YAML.load_file(file)
      end

      def file
        ARGV.first
      end
    end
  end
end

if HL7Server::Options.valid?
  EventMachine::run do
    Signal.trap("INT")  { EventMachine.stop }
    Signal.trap("TERM") { EventMachine.stop }
    host = HL7Server::Options.listen_host
    port = HL7Server::Options.listen_port
    EventMachine::start_server(host, port, HL7Server::MLLPServer)
  end
else
  puts "*"*90
  puts ""
  puts "Usage:"
  puts "#{File.basename(__FILE__)} config.yml"
  puts ""
  puts "*"*90
end
