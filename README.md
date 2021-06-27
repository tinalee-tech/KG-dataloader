# Knowledge Graph Dataloader 

## Introduction

This data loader program is intended to load raw data into the neo4j knowledge base from the multipledata sources including:
  1) 3D Structures from 4DN Consortium
  2) Genes and Transcripts from Ensemble dataset
  3) Gene Ontology from GO database
  4) Epigenomic Features from ENCODE and Roadmap
  5) Common Variants from GTEx
  6) Others:  As long as the file is formated as tabular data (similar to relation tables in relationaldatabase) or matrix data (first dimension of the data table refers to the attribute of the first node, seconddimension refers to the attribute of the second node, inner cells refer to the property of the edge con-necting the corresponding nodes), the dataloader will be able to work as expected. More details given in the following sections.
  
## Usage

Users could be able to run the dataloader using the following command in terminal:
```
python3 csv_loader.py [-i URI] <format file> <data file directory>
```

The required arguments are:
  * \<format file\>:  The required .format file for dataloader is specially defined.  Please refer to following sections for the detailed description.
  * \<data file\>: The directory of the data files to be loaded into the knowledge graph.

The optional arguments are:
  * [-i URI]: The URL of the neo4j knowledge base. Without specification, by default usesbolt://localhost:7687
  
Upon successful compilation of the program, users will first be prompted for neo4j database usernameand password in the terminal. After successful logging in, the dataloader will start to load data as in-tended.

## Examples

```
python3 csv_loader.py GTEx.format ./GTEx/GTEx_Analysis_v8_eQTL
```

This command will load every datafile under the path ./GTEx/GTEx\_Analysis\_v8\_eQTL into the database that's currently running on bolt://localhost:7687 using the given GTEx.format file. 

Note: it will be assumed that all file under this same directory share the same format.
