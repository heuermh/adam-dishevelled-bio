# adam-dishevelled-bio

###Hacking adam-dishevelled-bio

Install

 * JDK 1.8 or later, http://openjdk.java.net
 * Apache Maven 3.3.9 or later, http://maven.apache.org
 * dishevelled-bio, http://github.com/heuermh/dishevelled-bio

To build

    $ mvn install

###Running adam-dishevelled-bio using `spark-submit`

```bash
$ spark-submit \
    --class com.github.heuermh.adam.dishevelledbio.AdamDshFilterVcf \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryo.registrator=org.bdgenomics.adam.serialization.ADAMKryoRegistrator \
    target/adam-dishevelled-bio_2.10-0.19.1-SNAPSHOT.jar \
    --id rs201888535 \
    --single \
    input.vcf \
    output.vcf
```
