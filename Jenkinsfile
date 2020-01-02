node {
    def sbtHome = tool 'sbt-1.2.1'
    def sbt = "${sbtHome}/bin/sbt -no-colors -batch" 
    withCredentials([string(credentialsId: '95fa6854-242e-45f1-aed8-b4b0ef6dafbf', variable: 'APIKEY')]) {
    //withCredentials([[$class: 'UsernamePasswordMultiBinding', credentialsId: 'dev-athena-cloud-api-key', usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD']]) {
        sh "docker login -u iamapikey -p $APIKEY us.icr.io"
    }
    stage('Get GitHub repository') {
        checkout scm
    }
    stage('Build Spark Jar') {
         dir("${env.WORKSPACE}"){
              sh "${sbt} clean assembly"
         }
    }
    stage('Build Docker image') {
        //sh "docker build -f docker/Dockerfile ."
        app = docker.build('ddp-athena-dev/spark-cedp-demo', '-f docker/Dockerfile .')
    }
    stage('Push image to IBM Container Registry') {
        docker.withRegistry('https://us.icr.io', 'dev-athena-cloud-api-key') {
            app.push()
            app.push("${env.BUILD_NUMBER}")
        }
    }
    stage('Run job') {
        withCredentials([[$class: 'UsernamePasswordMultiBinding', credentialsId: 'dev-athena-cloud-api-key', usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD']]) {
            sh 'ibmcloud login --apikey $PASSWORD -a https://cloud.ibm.com -r us-east -c 3e160b5e625c4c33b8ef0842e6c68529 -g DDP --skip-ssl-validation'
            sh 'ibmcloud plugin install kubernetes-service'
            sh 'ibmcloud ks cluster ls'
            //-Djavax.net.ssl.trustStore='/somepath/truststore.jks'
            sh 'ibmcloud ks cluster config ddp-east-01'
            //sh 'keytool -import -v -trustcacerts -file lib/mongo-dst.pem -keystore /jre/lib/security/cacerts'
            env.KUBECONFIG='/home/jenkins/.bluemix/plugins/container-service/clusters/ddp-east-01/kube-config-wdc04-ddp-east-01.yml'
            sh 'kubectl delete sparkapp spark-pi -n athena-dev'
            sh "kubectl apply -f run-spark.yaml"
        }
    }
}      