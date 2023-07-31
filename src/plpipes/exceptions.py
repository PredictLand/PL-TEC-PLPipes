class AuthenticationError(Exception):
    pass

class CloudError(Exception):
    pass

class CloudFSError(CloudError):
    pass

class CloudAccessError(CloudError):
    pass
