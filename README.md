# R2RML Parser

A parser that can export relational database contents as RDF graphs, based on an [R2RML](http://www.w3.org/TR/r2rml/) mapping document. Contains an R2RML [mapping document](https://github.com/nkons/r2rml-parser/blob/master/src/main/resources/dspace/dspace-mapping.rdf) for the [DSpace](http://www.dspace.org/) institutional repository solution. Early results presented in [1]. More up-to-date results in the [wiki](https://github.com/nkons/r2rml-parser/wiki).

## Implementation

Written fully in Java, using Apache Jena, Spring, JUnit, and Maven. Tested against MySQL and PostgreSQL.

## Licence

This work is licensed under a Creative Commons Attribution-NonCommercial 3.0 Unported License.

http://creativecommons.org/licenses/by-nc/3.0/

You can use the work as long as you provide proper reference, e.g. a link to the project page and/or a reference to [1], and respect the license terms.

## Publications

[1] N. Konstantinou, D.E. Spanos, N. Houssos, N. Mitrou: Exposing Scholarly Information as Linked Open Data: RDFizing DSpace contents, In The Electronic Library, to appear (2013).