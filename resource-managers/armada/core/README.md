Armada Cluster Manager for Spark
---

Build
---
From the spark repository root: 
```bash
./build/sbt package -Pkubernetes -Parmada
```
You may have to adjust `JAVA_HOME` to suit your environment. Spark requires Java 17 and above. `-Parmada` tells
sbt to enable the armada project.

Test
---
```
./build/sbt armada/testOnly -Parmada
```
Runs all tests!
