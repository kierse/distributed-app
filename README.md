# README #

Attention: for Windows users, replace `gradlew` with `gradlew.bat`

## How to Use ##
- Set up EC2 instance and pull from master
- Update instances to [Java 9](#markdown-header-to-install-java9) if required
- Create a "server.txt" file in the project root with Public DNS addresses of each EC2 instance
- Initialize the instances with the following command:

_Note: substitute first argument with path to actual pem file_
```
> ./script/deploymentScript /Users/kierse/.ssh/eece513_aws.pem --ssh-keys
```
- Run the following command to build and deploy:

_Note: substitute populate pem flag with path to actual file_
```
> ./gradlew bootstrapAws -Ppem=/Users/kierse/.ssh/eece513_aws.pem
```
- In any EC2 instance home directory, start a cluster node:
```
> java -jar distributed-app-1.0.jar
```
- On startup, the node will print out its join address. This address can be used by other nodes wishing to join its cluster:

_Note: replace 127.0.0.1 with the join address obtained in the previous step_
```
> java -jar distributed-app-1.0.jar 127.0.0.1
```

**_Warning: starting a node without the address of an existing cluster will effectively create a second cluster._**

## fs513 ##
fs513 is a command line script that can be used to interact with the distributed filesystem.
```
usage: ./fs513 <action> [<option1> <option2>]

 Actions:
    put <localFileName> <remoteFileName>  (add a local file to fs513 with the given fs513 name)
    get <remoteFileName> <localFileName>  (fetch a fs513 file to the local machine)
    remove <remoteFileName>               (delete a file from fs513)

    grep [<arg1> <arg2> ... <argN>]       (search the fs513 server logs)
    locate <remoteFileName>               (list all machines (name / id / IP address) of the servers that contain a copy of the file)
    ls                                    (list all files in fs513)
    lshere                                (list all fs513 files stored on the local machine)

    --help                                (print this message)
```
### Supported fs513 commands ###
##### add file #####
```
./fs513 put foo.txt foo/bar/baz
```
##### get file #####
```
./fs513 get foo/bar/baz foo.txt
```
##### remove file #####
```
./fs513 remove foo/bar/baz
```
##### grep server logs #####
```
./fs513 grep -i "no files to sync""
```
##### locate file #####
```
./fs513 locate foo/bar/baz
```
##### list files in filesystem #####
```
./fs513 ls
```
##### list files on current machine #####
```
./fs513 lshere
```

## Tests ##

#### Unit Tests ####
```
 > ./gradlew test
```
#### Distributed Tests ####
```
 > ./gradlew distributedTest
```

## UPGRADE FROM JAVA7/8 TO JAVA9 ###

### To install Java9 ###

```
> wget --no-cookies --no-check-certificate --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/9.0.4+11/c2514751926b4512b076cc82f959763f/jdk-9.0.4_linux-x64_bin.rpm
> sudo yum install jdk-9.0.4_linux-x64_bin.rpm
```

### To remove old Java versions ###

```
> sudo yum remove java-1.8.0-openjdk
> sudo yum remove java-1.7.0-openjdk
```
