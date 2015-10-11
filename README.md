avroflat
==============

A Python utility script to convert an apache avro avpr file into several schemas and flatten them at the same time (including dependent schemas into one schema which often causes some headache with non standard apache avro library languages)

Dependencies
===============

1. Tested with python 2.7*
2. Apache Avro python libraries needs to be installed

Running it
================

The script accepts 2 commandline parameters

1. Path to input avpr file
2. Path to outputfolder where schemas should be stored

e.g. python avro_flat "TEST1.avpr" "~/resultschemas"



