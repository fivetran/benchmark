# Connect to an EC2 instance so we can run 11-BenchmarkRedshift.sh reliably
HOST=ec2-18-219-222-190.us-east-2.compute.amazonaws.com
PEM_FILE=~/.ssh/george-us-east-2.pem

# Copy benchmarking files to EC2 instance
zip -r redshift.zip \
      Warmup.sql \
      RedshiftCompileTimes.sh \
      200-PopulateRedshiftSimple.sh \
      201-PopulateRedshiftOptimized.sh \
      202-BenchmarkRedshift.sh \
      203-BenchmarkRedshiftAgain.sh \
      query

scp -i ${PEM_FILE} redshift.zip ec2-user@${HOST}:~
ssh -i ${PEM_FILE} ec2-user@${HOST}