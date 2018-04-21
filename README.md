# README #

Attention: for Windows users, replace `gradlew` with `gradlew.bat`

## How to Use ##
1. Set up EC2 instance and pull from master
2. Update instances to [Java 9](#markdown-header-to-install-java9) if required
3. Create a "Server.txt" file in the project root with Public DNS addresses of each EC2 instance
4. Initialize the instance with the following command:
    ```
    > ./script/deploymentScript /Users/kierse/.ssh/eece513_aws.pem --ssh-keys
    ```
5. Run the following command to build and deploy:<br />
_Note: this will build and deploy fs513 and the grep service_
    ```
    > ./gradlew bootstrapAws -Ppem=/Users/kierse/.ssh/eece513_aws.pem
    ```
6. In any EC2 instance home directory, start a cluster node:
    ```
    > java -jar distributed-app-1.0.jar
    ```
7. On startup, the node will print out its join address. This address can be used by other nodes wishing to join its cluster:
    ````
    > java -jar distributed-app-1.0.jar 127.0.0.1
    ````
Note: starting a node without the address of an existing cluster will effectively create a second cluster.

## To grep node logs ##
7. Follow instructions found [here](https://bitbucket.org/eece513/distributed-grep) and install distributed-grep on each EC2 instance.

Note: will need to be running code 'Assignment02' tag or later

## Auxiliary Commands ##

### Delete Artifacts ###
```
 > ./gradlew clean 
```

### Build Archive (.jar) ###

```
 > ./gradlew jar
```

#### Runs Distributed Tests ####
```
 > ./gradlew distributedTest
```

#### Runs Unit Tests ####
```
 > ./gradlew test
```
## SETUP LOCATE COMMAND ###
### To install Locate ###
```
> sudo yum install mlocate
> sudo updatedb
```
### To make locat.db visible ###
```
> updatedb --require-visibility 0 -o ~/.locate.db
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
