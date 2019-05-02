properties([[$class: 'BuildDiscarderProperty', strategy: [$class: 'LogRotator', artifactNumToKeepStr: '2', numToKeepStr: '2']]])

node {
    def projectName = "redplex"

    def gopath = pwd() + "/gopath"
    def projectDir = "${gopath}/src/github.com/mixer/${projectName}"

    env.GOPATH = "${gopath}"
    env.PATH = env.PATH + ":${gopath}/bin"

    try {
        sh "mkdir -p '${projectDir}'"
        dir (projectDir) {
            stage("Checkout") {
                checkout scm
            }
            stage("Initialize services") {
                sh 'sudo /usr/bin/systemctl start redis'
            }
            stage("Prepare") {
                sh 'go get github.com/kardianos/govendor'
                sh 'go get golang.org/x/lint/golint'
                sh 'govendor sync'
            }
            stage("Test") {
                sh 'make check'
            }
            stage("Compile") {
                sh 'make redplex'
            }
            stage("artifacts") {
                archiveArtifacts artifacts: "${projectName}", fingerprint: true
            }
            currentBuild.result = "SUCCESS"
        }
    } catch(e) {
        currentBuild.result = "FAILURE"
        throw e
    }
}
