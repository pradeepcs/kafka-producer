pipeline {
  agent any
  stages {
    stage('build') {
      steps {
        sh '''echo $M2_HOME
echo $PATH
mvn clean install -DskipTests'''
      }
    }

  }
}