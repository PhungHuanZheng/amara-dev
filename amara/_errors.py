"""
This module provides Exception types used throughout `amara`.
"""

class NotInitiatedError(Exception):
    """
    Property of class exists but true value has not been initiated
    by another method.
    """
    pass