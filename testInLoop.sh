#!/bin/bash

OUT=/tmp/fastests.log
INT=1

while [ 1 == 1 ];
do
echo "Started at  `date`" | tee -a ${OUT}
echo "===================" | tee -a ${OUT}
echo " TIME = ${INT}     " | tee -a ${OUT}
echo "===================" | tee -a ${OUT}
((INT++))
dotnet test test/FastTests &>> ${OUT} 2>&1 &
wait \%1
RC=$?
echo "Ended at `date`" | tee -a ${OUT}
echo "===================" | tee -a ${OUT}
echo "Exited with rc=${RC}" | tee -a ${OUT}
echo " " &>> ${OUT}
done


