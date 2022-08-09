yum update -y
yum install docker
wget https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m) 
mv docker-compose-$(uname -s)-$(uname -m) /bin/docker-compose
chmod -v +x /bin/docker-compose
systemctl enable docker.service
systemctl start docker.service
