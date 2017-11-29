# Connect to an EC2 instance so we can run 11-BenchmarkRedshift.sh reliably
HOST=107.23.87.86
PEM_FILE=/Users/george/Downloads/george-test.pem

scp -i ${PEM_FILE} -r *.* ec2-user@${HOST}:~
scp -i ${PEM_FILE} -r */ ec2-user@${HOST}:~
ssh -i ${PEM_FILE} ec2-user@${HOST}