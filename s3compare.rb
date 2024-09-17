# This script is an example of how you could compare local files with remote ones in S3
# While on it's face this might seem trivial, it actually takes a few extra steps
# to avoid downloading the object every time you would need to compare local and remote data
# In this example we solve this by attaching a special metadata sha512 of the object plaintext
# to the object. Then we run an API call to inspect the digest of the remote object instead of
# downloading the entire object. We could do this on a cron, say every 30 minutes, and enforce
# the desired state based on S3 metadata.
#
# In the event that the local file does not exist AND the object has no associated hash
# then the object is downloaded, a hash is computed, and digets metadata is initialized.
#
# If both the file exists and the remote file exists but the hash is not present.
# then download both, compare, and if they match keep one.
#
# No match? No metadata? Keep both.



require 'aws-sdk-s3'
require 'digest'
require 'fileutils'

# Testing only
aws_region = 'INSERT REGION'
bucket_name = 'INSERT BUCKET'
object_key = 'INSERT OBJECT'
local_file_path = './FILENAME'
metadata_key = 'sha512'

# Delete this
s3 = Aws::S3::Client.new(
  region: aws_region,
  credentials: Aws::Credentials.new('DONTDOTHIS', 'STOPTHAT')
)

def compute_sha512(file_path)
  sha512 = Digest::SHA512.new
  File.open(file_path, 'rb') do |file|
    while chunk = file.read(8192)
      sha512.update(chunk)
    end
  end
  sha512.hexdigest
end

def upload_file_with_metadata(s3, bucket_name, object_key, file_path, sha512_hash, metadata_key)
  s3.put_object(
    bucket: bucket_name,
    key: object_key,
    body: File.open(file_path),
    metadata: { metadata_key => sha512_hash }
  )
end

def download_object(s3, bucket_name, object_key, local_file_path)
  s3.get_object(response_target: local_file_path, bucket: bucket_name, key: object_key)
  puts "Object downloaded to #{local_file_path}"
end

def replace_local_file(local_file_path, object_key, bucket_name, s3)
  FileUtils.mv(local_file_path, local_file_path + '.bak')
  s3.get_object(response_target: local_file_path, bucket: bucket_name, key: object_key)
  puts "Replaced local file with the object from S3."
end

# Check if the metadata hash exists
def metadata_key_exists?(s3, bucket_name, object_key, metadata_key)
  begin
    response = s3.head_object(bucket: bucket_name, key: object_key)
    metadata = response.metadata
    if metadata.key?(metadata_key)
      puts "Metadata key '#{metadata_key}' exists with value '#{metadata[metadata_key]}'"
      true
    else
      puts "Metadata key '#{metadata_key}' does not exist."
      false
    end
  rescue Aws::S3::Errors::NotFound
    puts 'Object not found in the specified bucket.'
    false
  rescue Aws::S3::Errors::ServiceError => e
    puts "Service error: #{e.message}"
    false
  rescue StandardError => e
    puts "An error occurred: #{e.message}"
    false
  end
end

# Compare the local file's hash with the hash stored in S3 metadata
def compare_and_update(s3, bucket_name, object_key, local_file_path, metadata_key)
  begin
    metadata_exists = metadata_key_exists?(s3, bucket_name, object_key, metadata_key)

    if File.exist?(local_file_path)
      local_hash = compute_sha512(local_file_path)
      if metadata_exists
        s3_hash = s3.head_object(bucket: bucket_name, key: object_key).metadata[metadata_key]
        if local_hash == s3_hash
          puts "Hashes match: #{local_hash}"
        else
          puts "Hashes do not match."
          puts "S3 hash:    #{s3_hash}"
          puts "Local hash: #{local_hash}"
          puts "Replacing local file with S3 object..."
          #download_object(s3, bucket_name, object_key, local_file_path)
          replace_local_file(local_file_path, object_key, bucket_name, s3)
        end
      else
        # Metadata does not exist in S3
        puts "Metadata does not exist in S3. But local file does..."
        puts "Comparing..."
        wtf(s3, bucket_name, object_key, local_file_path, metadata_key)
      end
    else
      # File does not exist on disk
      if metadata_exists
        # Metadata exists in S3
        puts "File does not exist on disk. Downloading object..."
        download_object(s3, bucket_name, object_key, local_file_path)
        local_hash = compute_sha512(local_file_path)
        s3_hash = s3.head_object(bucket: bucket_name, key: object_key).metadata[metadata_key]

        if local_hash == s3_hash
          puts "Hashes match after downloading the file."
        else
          puts "Hashes do not match after downloading the file. Local hash: #{local_hash}, S3 hash: #{s3_hash}"
          # TODO: Handle mismatch as needed. This should not happen but maybe AWS is drunk idk)
        end
      else
        # Metadata does not exist and file does not exist
        puts "Metadata does not exist in S3 and file does not exist on disk. Downloading object..."
        download_object(s3, bucket_name, object_key, local_file_path)
        sha512_hash = compute_sha512(local_file_path)
        upload_file_with_metadata(s3, bucket_name, object_key, local_file_path, sha512_hash, metadata_key)
        puts "Object downloaded and SHA-512 hash metadata added to S3 object."
      end
    end
  rescue Aws::S3::Errors::ServiceError => e
    puts "Service error: #{e.message}"
  rescue StandardError => e
    puts "An error occurred: #{e.message}"
  end
end


#If we have a local file, and remote object but don't have a metadata hash
#Then download the object to .new and compare hash to existing file.
#
#Hashes match? nice delete one
#
#Hashes don't match? Leave both.
def wtf(s3, bucket_name, object_key, local_file_path, metadata_key)
  new_file_path = local_file_path + '.new'

  # Download the S3 object to a new file
  download_object(s3, bucket_name, object_key, new_file_path)

  if File.exist?(local_file_path)
    local_hash = compute_sha512(local_file_path)
    new_file_hash = compute_sha512(new_file_path)

    if local_hash != new_file_hash
      # Rename existing file to .bak and new file to the original name
      replace_local_file(local_file_path, object_key, bucket_name, s3)
      #compare_and_update(s3, bucket_name, object_key, local_file_path, metadata_key)
      puts "Hash mismatch. Trusting S3 Object."
      puts "Replaced existing file with new file. Old file backed up as .bak."
      puts "Metadata updated with new file from S3."
      upload_file_with_metadata(s3, bucket_name, object_key, local_file_path, local_hash, metadata_key)
    else
      # If hashes match, delete the new file and update the metadata
      FileUtils.rm(new_file_path)
      puts "Hashes match. New file discarded. Metadata hash updated."
      upload_file_with_metadata(s3, bucket_name, object_key, local_file_path, local_hash, metadata_key)
    end
  else
    # If no file exists at all (how did we get here?)
    puts "No existing file."
  end
end

# Run the script
compare_and_update(s3, bucket_name, object_key, local_file_path, metadata_key)
