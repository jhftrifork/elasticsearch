node { 
    git 'https://github.com/mesos/elasticsearch.git'
    stage 'Compile'

    //step([$class: 'SlackNotifier'])
    sh './gradlew clean compileJava'

    stage 'QA'
    sh './gradlew build jacocoTestReport sonarRunner'
    step([$class: 'JUnitResultArchiver', testResults: '**/test-results/*xml'])

    sh './gradlew systemTest'
    step([$class: 'JUnitResultArchiver', testResults: '**/test-results/*xml'])

    stage 'Release'
    sh './gradlew publishDockerImageWithLatest'
}
