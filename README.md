# aws-s3-tools-ruby

 This script is an example of how you could compare local files with remote ones in S3
 While on it's face this might seem trivial, it actually takes a few extra steps
 to avoid downloading the object every time you would need to compare local and remote data
 In this example we solve this by attaching a special metadata sha512 of the object plaintext
 to the object. Then we run an API call to inspect the digest of the remote object instead of
 downloading the entire object. We could do this on a cron, say every 30 minutes, and enforce
 the desired state based on S3 metadata.

 In the event that the local file does not exist AND the object has no associated hash
 then the object is downloaded, a hash is computed, and digets metadata is initialized.

 If both the file exists and the remote file exists but the hash is not present.
 then download both, compare, and if they match keep one.

 No match? No metadata? Keep both.

