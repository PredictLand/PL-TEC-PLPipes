"""
plpipes.exceptions

This module defines custom exception classes for handling various
error scenarios in the plpipes package. These exceptions extend
the built-in Exception class and provide better context for error
handling and debugging.

"""

class AuthenticationError(Exception):
    """
    Exception raised for authentication-related errors.
    """
    pass

class CloudError(Exception):
    """
    Base exception class for all cloud-related errors.
    """
    pass

class CloudFSError(CloudError):
    """
    Exception raised for file system errors related to cloud operations.
    """
    pass

class CloudAccessError(CloudError):
    """
    Exception raised for access-related errors when interacting with cloud services.
    """
    pass
