# R2RML Parser

An R2RML implementation that can export relational database contents as RDF graphs, based on an [R2RML](http://www.w3.org/TR/r2rml/) mapping document. Contains an R2RML [mapping document](https://github.com/nkons/r2rml-parser/blob/master/dspace/dspace-mapping.rdf) for the [DSpace](http://www.dspace.org/) institutional repository solution.

For more information, please [visit the wiki](https://github.com/nkons/r2rml-parser/wiki).

Please send any feedback/questions by email to [nkons@live.com](mailto:nkons@live.com), by direct message (dm) via twitter to [@nkonstantinou](https://twitter.com/nkonstantinou), or [open a new issue](https://github.com/nkons/r2rml-parser/issues). We'll be happy to discuss how to export your data into RDF.

If you use R2RML Parser, please cite it in your publications as follows:
```bibtex
@inproceedings{Konstantinou2014,
 author = {Konstantinou, Nikolaos and Kouis, Dimitris and Mitrou, Nikolas},
 title = {Incremental Export of Relational Database Contents into RDF Graphs},
 booktitle = {Proceedings of the 4th International Conference on Web Intelligence, Mining and Semantics (WIMS14)},
 series = {WIMS '14},
 year = {2014},
 location = {Thessaloniki, Greece},
 doi = {10.1145/2611040.2611082},
 publisher = {ACM}
} 
```

## Implementation details

R2RML implementation written fully in Java 7, using Apache Jena 2.11, Spring 4.0, JUnit 4.9, and Maven 3.1. Tested against MySQL 5.6, PostgreSQL 9.2 and Oracle 11g.

## Licence

This work is licensed under a Creative Commons Attribution-NonCommercial 4.0 Unported License.

http://creativecommons.org/licenses/by-nc/4.0/

You are free to use and distribute this work as long as you provide proper reference and respect the license terms.

## Publications

You can find in the wiki a list of [publications](https://github.com/nkons/r2rml-parser/wiki/Publications) based on the tool.
