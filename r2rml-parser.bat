@echo off
echo This is R2RML Parser 0.6-alpha
java -Xms128m -Xmx1024m -cp "./*;./lib/*;" gr.seab.r2rml.beans.Main %1
echo R2RML Parser 0.6-alpha. Done.
