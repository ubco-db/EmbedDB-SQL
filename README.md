# EmbedDB SQL Converter

The code contained in this repository uses a modified version of the [HyperSQL](https://hsqldb.org/) source code to convert basic SQL queries into C code that can be executed on [EmbedDB](https://github.com/ubco-db/EmbedDB). 

## Repo structure

```
.
├── build                 # Compiled executable JAR files
├── lib                   # Libraries for HSQLDB and the tests
├── src
│   ├── org.hsqldb        # Modified HSQLDB source code
│   ├── SQLConverter      # Converter code
│   └── *.java            # Mains that compile into the build folder
├── tests                 # Tests for src/SQLConverter
└── README.md
```

## Running the compiled JARs

To run the compiled jars, you can use `java -jar` to run any jar located in a subdirectory of [`build`](./build).

Example:

```bash
java -jar build/converter-GUI/converter-GUI.jar
```
