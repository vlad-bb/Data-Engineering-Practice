## Build image

```bash
docker build -t website-img .
```

## Run Docker container

```bash
docker container run -p 8080:80 --name website-container website-img
```

## Go to

http://localhost:8080/

## Login in DockerHub

```bash
docker login
```

## Set tag to image

```bash
docker tag website-img vladbb1990/website-img:0.0.1
```

## Push image to DockerHub

```bash
docker push vladbb1990/website-img:0.0.1
```

## Image ready for use

https://hub.docker.com/repository/docker/vladbb1990/website-img/tags/0.0.1/sha256-bba46cd53511eeba638050a23ef6ec48d529e964afd5749a94ebcec93aafcaa1
