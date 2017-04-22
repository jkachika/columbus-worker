#!/usr/bin/env python
"""The package provides APIs that help connect with Google cloud services. The library methods are intended to be used
in the code composed for Components and Combiners of the Columbus platform. Several methods in the library have the
parameter `user_settings` - these are sent to the worker by the Columbus master, so code composed inside Components
and Combiners will have access to the user settings internally and the parameter can be ignored.
"""