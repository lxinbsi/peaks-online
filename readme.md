**PEAKS Online**

Distributed computing platform for parallel DDA/DIA 
protein/peptide identification 

Follow the instructions below to setup your own PEAKS Online server locally, 
alternatively, contact lxin@bioinfor.com to get a free account on our public 
PEAKS Online server to try the software 

**Operating System**

PEAKS Online runs on both Windows & Linux, it has been tested
on Windows 7, 10, and Ubuntu 18.04 and 20.04

**Dependency**

Apache Cassandra 3+, and Oracle JDK/JRE 1.8,
make sure Oracle JDK/JRE 1.8 is the default JAVA executable
on your operating system and it's in the system path

**Dependency Setup**
- Set up a Cassandra Cluster, you may use a single machine Cassandra instance for testing,
  but it may not be powerful enough to process large amount of data
  - You can find instructions for setting up a Cassandra Cluster here: https://cassandra.apache.org/doc/latest/cassandra/getting_started/installing.html
- Download the uniprot database taxonomy files onto your testing machine
  - Uniprot Speclist: https://ftp.uniprot.org/pub/databases/uniprot/knowledgebase/docs/speclist.txt
  - Uniprot Taxdmp: https://ftp.ncbi.nih.gov/pub/taxonomy/taxdmp.zip (this folder will need to be unzipped after download)
- For NCBI database support you will need to find and download the NCBI prot.accession2taxid.gz and gi_taxid_prot.dmp files.
- Download pytorch dependencies onto your testing machine
  - Pytorch version should be 1.7.1, instructions on how to download and compile can be found here: https://pytorch.org/get-started/previous-versions/   

**Run PEAKS Online from IntelliJ IDEA**
- Import peaks-online as a Maven project into IntelliJ IDEA
  - Copy taxonomy files downloaded in Dependency Setup into folder in the root directory of your project called "taxonomy" 
  - Copy the pytorch dependencies downloaded in Dependency Setup into a nested folder in the root directory of your project called "mllib/pytoch"
- Modify master-config.conf. This configuration file will be used to change parameters for your
  PEAKS Online master instance. Make changes to the following parameters:
  - cluster.seed-nodes: Change the localhost in this to the IP address for the master machine
  - remote.netty.tcp.hostname: Change the localhost in this to the same IP address you used for cluster.seed-nodes
  - initial-contacts: Change the localhost in this to the same IP address used in cluser.seed-nodes as well
  - io.reader.taxonomy: Change these paths to point to the downloaded taxonomy files
  - contact-points: Change this IP address to point to your Cassandra cluster. If you are running multiple Cassandra 
  instances as a cluster, you should enter each Cassandra instances IP address as a comma separated list,
    for example: "127.0.0.1,127.0.0.2". 
- Modify worker-config.conf. This configuration file will be used to change parameters for your
  PEAKS Online worker instance. Make changes to the following parameters: 
  - remote.netty.tcp.hostname: Change the localhost in this to the IP address for the worker machine. 
  - initial-contacts: Change the localhost in this to point to the IP address of the master node. This should
    be the IP you used in the master-config.conf.
  - contact-points: Change this IP address to point to your Cassandra cluster. If you are running multiple Cassandra
    instances as a cluster, you should enter each Cassandra instances IP address as a comma separated list,
  - max-cpu: This is the number of CPU threads that can be utilized by PEAKS Online. Set this to an integer value 
    no greater than 16. 
  - max-memory: This is the amount of memory available to PEAKS Online workers. The recommended amount is 2.5x the 
    max-cpu value. 
  - gpus: If you have a GPU on the worker machine you can attach it to help speed up DIA DB workflow. Enter the gpu name
  inside the square brackets, for example: ["gpu0"]. 
- Create a new run configuration in IntelliJ for the master node
  - Use com.bsi.peaks.server.Launcher as the main class
  - Pass in the program argument "-c master-config.conf"
- Create a new run configuration in IntelliJ for the worker node
  - Use com.bsi.peaks.server.Launcher as the main class
  - Pass in the program argument "-c worker-config.conf"
- Start master node in IntelliJ using the created Master run configuration
- Start worker node in IntelliJ using the created Worker run configuration
- Once the master node has been started successfully, the web UI can be
  accessed at http://localhost:4000 or at http://<master-ip-here>:4000
  - You can access the server with the follow credentials:
    - Username: onlineuser
    - Password: onlineuser
  - These credentials can be changed once you have logged into the server
  