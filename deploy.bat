cd ./image/
docker build -t docker-image:test . && (
    cd ../
    cdk bootstrap && (
        cdk deploy
    )
)
    