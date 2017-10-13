#!/bin/sh

# Makes a copy of the jar file so the jar file can be recompiled
# during runtime without problems

mkdir -p run
rsync -aq jar/Jelectrum.jar run/Jelectrum-run.jar
java -Xmx4g -jar run/Jelectrum-run.jar jelly.conf


