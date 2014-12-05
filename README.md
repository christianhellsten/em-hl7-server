em-hl7-server
=============

An HL7 server written in Ruby, which is a proof-of-concept at this point. Uses EventMachine. Writes HL7 messages to database.

TODO:

 * Proper parsing of HL7 messages in TCP stream (state-machine based)
 * Validation of HL7 message format
 * Less blocking of EM
 * ACK/NAK
