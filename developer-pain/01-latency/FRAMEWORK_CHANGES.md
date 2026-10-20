Two one-line additions in the OpenMessaging Benchmark repo:

pom.xml, inside <modules>:
    <module>driver-iggy</module>

benchmark-framework/pom.xml, inside <dependencies>:
    <dependency>
      <groupId>${project.groupId}</groupId>
      <artifactId>driver-iggy</artifactId>
      <version>${project.version}</version>
    </dependency>
