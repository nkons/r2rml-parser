# R2RML Parser

An R2RML implementation that can export relational database contents as RDF graphs, based on an [R2RML](http://www.w3.org/TR/r2rml/) mapping document. Contains an R2RML [mapping document](https://github.com/nkons/r2rml-parser/blob/master/dspace/dspace-mapping.rdf) for the [DSpace](http://www.dspace.org/) institutional repository solution.

For more information, please see the [wiki](https://github.com/nkons/r2rml-parser/wiki).

Please send any feedback or questions to [nkons@live.com](mailto:nkons@live.com), or [open a new issue](https://github.com/nkons/r2rml-parser/issues). Happy to discuss how to get value from your data.

If you use R2RML Parser, please cite it in your publications as follows:
```bibtex
@article{Konstantinou2014,
author = {Nikolaos Konstantinou and Dimitrios-Emmanuel Spanos and Nikos Houssos and Nikolaos Mitrou},
title = {Exposing scholarly information as Linked Open Data: RDFizing DSpace contents},
journal = {The Electronic Library},
volume = {32},
number = {6},
pages = {834-851},
year = {2014},
doi = {10.1108/EL-12-2012-0156}
}
```

### Implementation details

R2RML implementation written fully in Java 7, using Apache Jena 2.11, Spring 4.0, JUnit 4.9, and Maven 3.1. Tested against MySQL 5.6, PostgreSQL 9.2 and Oracle 11g.

### Licence

This work is licensed under the Apache License 2.0.

### Publications

You can find in the wiki a list of [publications](https://github.com/nkons/r2rml-parser/wiki/Publications) based on the tool.
