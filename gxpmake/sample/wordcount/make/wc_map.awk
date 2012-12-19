#!/usr/bin/awk -f
{ for (i = 1; i <= NF; i++) print $i,1; }
