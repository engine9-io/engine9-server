# Packet Server

Engine9 Packets are the fundamental data structure to transmit and store information.  Packets are zip files that contain a manifest.js file, data files, and optionally statistics files.  Packets can come from many different locations, the packet-server code exists to abstract access to those packets.  Eventually it should be able to leverage a variety of mechanisms, but right now it assumes you're using CloudZip, which is a high performance optimized partial zip extractor:

https://github.com/ozkatz/cloudzip

## Installation:
Compile and deploy CloudZip such that it is available in the path. The Engine9 api server runs cloudzip as a child process when required.


