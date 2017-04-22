#!/usr/bin/env python
"""A library for distributed execution of workflows submitted through Columbus. The library helps connect with Google
cloud services through the API methods provided in the service package. The library methods are intended to be used in
the code composed for Components and Combiners of the Columbus platform. Several methods in the library have the
parameter `user_settings` - these are sent to the worker by the Columbus master, so code composed inside Components
and Combiners will have access to the user settings internally and the parameter can be ignored.
"""

__version__ = '0.1.0'
