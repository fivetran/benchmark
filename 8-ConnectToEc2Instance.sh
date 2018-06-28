# Connect to an EC2 instance so we can run 11-BenchmarkRedshift.sh reliably
HOST=ec2-34-205-89-133.compute-1.amazonaws.com
PEM_FILE=~/.ssh/george-us-east.pem

# Copy benchmarking files to EC2 instance
zip -r redshift.zip \
      Warmup.sql \
      RedshiftTiming.sql \
      200-PopulateRedshiftSimple.sh \
      201-PopulateRedshiftOptimized.sh \
      202-BenchmarkRedshift.sh \
      query

scp -i ${PEM_FILE} redshift.zip ec2-user@${HOST}:~
ssh -i ${PEM_FILE} ec2-user@${HOST}