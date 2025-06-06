Of course. Here is the content of that page converted into a clean Markdown file, ready for your repository.

I've formatted the code blocks with syntax highlighting and preserved all the links and images.

---

```markdown
# Using API Keys with the cfbd API

The [CollegeFootballData.com](https://collegefootballdata.com/) API is a free and open-source API for college football data. All of the data, documentation, and code is free to use under an [MIT license](https://opensource.org/licenses/MIT).

The API has been and will always be free to use. In the past, no API key was required for use. However, as the API has grown in popularity, the need for API keys has become a necessity.

## So, why do I need an API key?

API keys are now required for all API calls. They are used for identification and not for billing purposes. The API is and will continue to be free. However, in order to keep the API free for everyone, I need some way to identify and contact users of the API who are causing issues. In the past, this has meant having to shut down the API for everyone due to a single user's errant code. With API keys, I can identify the user, disable their key, and contact them to resolve the issue without affecting other users.

You can find more information at [collegefootballdata.com](https://collegefootballdata.com).

## How do I get an API key?

First, you will need to go to collegefootballdata.com and [register for a new account](https://collegefootballdata.com/register). After you have registered and verified your account, you can log in and navigate to your [profile page](https://collegefootballdata.com/account). Here you will see a section for your API key. You can create a new key or revoke an old one from this page.

![Screenshot of API key on profile page](https://blog.collegefootballdata.com/content/images/2020/07/key_screenshot.png)

## How do I use my API key?

Once you have an API key, you will need to include it in the `Authorization` header for all of your API requests. The key should be preceded by the word `Bearer` and a space.

```
Authorization: Bearer <YOUR_API_KEY>
```

Here are some examples of how to do this in a few different languages.

### cURL

```bash
curl "https://api.collegefootballdata.com/teams" -H "Authorization: Bearer <YOUR_API_KEY>"
```

### Python

The easiest way to get started with Python is to use the `cfbd` Python package. The package is open source and can be found on GitHub here: [cfbd](https://github.com/CFBD/cfbd-python).

You can install the package via `pip`:

```bash
pip install cfbd
```

Here is an example of how to use the package:

```python
import cfbd
from __future__ import print_function
import time
import sys

# Configure API key authorization: BearerAuth
configuration = cfbd.Configuration()
configuration.api_key['Authorization'] = '<YOUR_API_KEY>'
configuration.api_key_prefix['Authorization'] = 'Bearer'

# create an instance of the API class
api_instance = cfbd.TeamsApi(cfbd.ApiClient(configuration))

try:
    # Team information
    api_response = api_instance.get_teams()
    print(api_response)
except ApiException as e:
    print("Exception when calling TeamsApi->get_teams: %s\n" % e)
```

### R

The easiest way to get started with R is to use the `cfb-r` R package. The package is open source and can be found on GitHub here: [cfb-r](https://github.com/meysubb/cfb-r).

You can install from GitHub with:

```r
# You can install using the pacman package:
if (!requireNamespace('pacman', quietly = TRUE)){
  install.packages('pacman')
}
pacman::p_load_gh("meysubb/cfb-r")
```

Here is an example of how to use the package:

```r
# Load the library
library(cfb.r)

# Set API key
Sys.setenv(CFBD_API_KEY = "<YOUR_API_KEY>")

# Get team listings
cfb_teams()
```

If you have any questions, feel free to ask in our [Discord server](https://discord.gg/wXZ9ka2).

---
*This file was generated from the blog post at: https://blog.collegefootballdata.com/using-api-keys-with-the-cfbd-api/*
```