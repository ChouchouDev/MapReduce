### Objective
Recommendation Engines: "Customers who bought this item also bought"

### Execution of program  
#### Input data  
```
>hadoop fs -cat /user/Miao1/alsoBought/input/input1.txt
Artificial Intelligence, Deep Learning, Machine Learning
Artificial Intelligence, Semantic Web
Artificial Intelligence, Iot,
Artificial Intelligence, Iot
```
```
>hadoop fs -cat /user/Miao1/alsoBought/input/input2.txt
Iot, Data Mining
Iot, Data Mining
Artificial Intelligence, Semantic Web, Iot
```
#### Execution  
```
>hadoop com.sun.tools.javac.Main AlsoBought.java
>jar cf alsoBought.jar AlsoBought*.class
>hadoop jar alsoBought.jar AlsoBought /user/Miao1/alsoBought/input /user/Miao1/alsoBought/output
```
#### Output  
```
>hadoop fs -cat /user/Miao1/alsoBought/output/part-r-00000
Artificial Intelligence	{Iot=3, Semantic Web=2, Machine Learning=1, Deep Learning=1}
Data Mining	{Iot=2}
Deep Learning	{Machine Learning=1, Artificial Intelligence=1}
Iot	{Artificial Intelligence=3, Data Mining=2, Semantic Web=1}
Machine Learning	{Deep Learning=1, Artificial Intelligence=1}
Semantic Web	{Artificial Intelligence=2, Iot=1}
```
