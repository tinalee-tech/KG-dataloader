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
python load_data.py [-i URI] <format file> <data files>
```

The required arguments are:
  * \<format file\>:  The required .format file for dataloader is specially defined.  Please refer to following sections for the detailed description.
  * \<data file\>: The directory including all the data files to be loaded into the knowledge graph.

The optional arguments are:
  * [-i URI]: The URL of the neo4j knowledge base. Without specification, by default usesbolt://localhost:7687
  
Upon successful compilation of the program, users will first be prompted for neo4j database usernameand password in the terminal. After successful logging in, the dataloader will start to load data as in-tended.
