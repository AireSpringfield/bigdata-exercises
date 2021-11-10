clean (){
    rm *.jar
    rm *.class
}

build (){
    checkArgument $1
    clean
    javac $1.java -cp $(hadoop classpath)
    jar cvf $1.jar *.class 
}

run(){
    checkArgument $1
    hadoop fs -rm -r -f /tmp/gut01/
    time yarn jar $1.jar $1 /gutenberg_x0.1.txt /tmp/gut01/
}

checkArgument(){
    if [ -z "$1" ]; then
        echo "No input file name"; exit
    fi
}


if [ "$1" = "build" ]; then
    build $2
elif [ "$1" = "clean" ]; then
    clean
elif [ "$1" = "run" ]; then
    run $2
else
    echo "Commmand should be 'build', 'clean' or 'run'"
fi

