# Connect to an EC2 instance so we can run 11-BenchmarkRedshift.sh reliably
HOST=ec2-35-166-93-125.us-west-2.compute.amazonaws.com
PEM_FILE=~/.ssh/george.pem

# Copy benchmarking files to EC2 instance
zip -r redshift.zip \
      Warmup.sql \
      RedshiftTiming.sql \
      200-PopulateRedshift.sh \
      201-BenchmarkRedshift.sh \
      query

scp -i ${PEM_FILE} redshift.zip ec2-user@${HOST}:~
ssh -i ${PEM_FILE} ec2-user@${HOST}