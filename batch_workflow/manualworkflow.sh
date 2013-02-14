#!/bin/sh

###################
# options
###################

INPUTFILE=$1
OUTPUTDIR="output/"
J2KSETTINGS="-I -p RPCL -n 7 -EPH -SOP -c [256,256],[256,256],[128,128],[128,128],[128,128],[128,128],[128,128] -b 64,64 -M 1 -r 320.000,160.000,80.000,40.000,20.000,11.250,7.000,4.600,3.400,2.750,2.400,1.000"
JP2ENC=/home/will/local/bin/image_to_j2k
EXIFTOOL=/usr/bin/exiftool
JPYLYZER=/home/will/local/bin/jpylyzer
LIBRARY_PATH=/home/will/local/lib
MATCHBOX_EXTRACT=/home/will/local/bin/extractfeatures
MATCHBOX_COMPARE=/home/will/local/bin/compare

###################
# constants
###################

OUTPUTFILE=$INPUTFILE.jp2
SIFTEXT=".SIFTComparison.feat.xml.gz"
PROFEXT=".ImageProfile.feat.xml.gz"
IMGEXT=".ImageHistogram.feat.xml.gz"
METAEXT=".ImageMetadata.feat.xml.gz"
SIFTCOMPEXT=".siftcomp.xml"
PROFCOMPEXT=".profcomp.xml"
JPYLYZEREXT=".jpylyzer.xml"
EXIFTOOLEXT=".exiftool.xml"

###################
# script
###################

#export LD_LIBRARY_PATH

#if the input file exists
if [ -e $INPUTFILE ] 
then 

#make the output directory
mkdir -p $OUTPUTDIR

#extract metadata from the original file
$EXIFTOOL -X $INPUTFILE > $INPUTFILE$EXIFTOOLEXT

#migrate the original file
LD_LIBRARY_PATH=$LIBRARY_PATH $JP2ENC -i $INPUTFILE -o $OUTPUTFILE $J2KSETTINGS

#extract metadata from the new file
$EXIFTOOL -X $OUTPUTFILE > $OUTPUTFILE$EXIFTOOLEXT

#extract jpylyzer data
$JPYLYZER $OUTPUTFILE > $OUTPUTFILE$JPYLYZEREXT

#extract matchbox features
LD_LIBRARY_PATH=$LIBRARY_PATH $MATCHBOX_EXTRACT $INPUTFILE
LD_LIBRARY_PATH=$LIBRARY_PATH $MATCHBOX_EXTRACT $OUTPUTFILE

#compare matchbox data
LD_LIBRARY_PATH=$LIBRARY_PATH $MATCHBOX_COMPARE $INPUTFILE$SIFTEXT $OUTPUTFILE$SIFTEXT > $OUTPUTFILE$SIFTCOMPEXT
LD_LIBRARY_PATH=$LIBRARY_PATH $MATCHBOX_COMPARE $INPUTFILE$PROFEXT $OUTPUTFILE$PROFEXT > $OUTPUTFILE$PROFCOMPEXT

#md5 the generated files
md5sum $INPUTFILE* > $INPUTFILE.md5

#zip the files together
zip $OUTPUTDIR$INPUTFILE.zip $INPUTFILE.*

#delete the rest of the files
rm $INPUTFILE$EXIFTOOLEXT $OUTPUTFILE $OUTPUTFILE$EXIFTOOLEXT $OUTPUTFILE$JPYLYZEREXT $OUTPUTFILE$SIFTCOMPEXT $OUTPUTFILE$PROFCOMPEXT $INPUTFILE.md5 $INPUTFILE$SIFTEXT $OUTPUTFILE$SIFTEXT $INPUTFILE$PROFEXT $OUTPUTFILE$PROFEXT $OUTPUTFILE$METAEXT $OUTPUTFILE$IMGEXT $INPUTFILE$METAEXT $INPUTFILE$IMGEXT  

fi

