package jelectrum;

import com.amazonaws.services.s3.AmazonS3Client;

public class LargeStorage
{
    private AmazonS3Client s3;

    public LargeStorage(Config config)
    {
        config.require("aws_key_id");
        config.require("aws_key_secret");
        config.require("s3_bucket");

    }


}

