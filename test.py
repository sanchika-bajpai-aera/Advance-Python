# def parse_full_name(full_name: str) -> tuple[str, str]:
  #  parts = full_name.split(" ", 1)
  #  first = parts[0]
  #  last = parts[1] if len(parts) > 1 else ""
  #  return (first, last) */

# Usage
#first, last = parse_full_name("John Doe")
#print(first)  # "John"
#print(last)   # "Doe"

# Split "owner/repo" into two parts
def parse_repo(repo: str) -> tuple[str, str]:
    owner, name = repo.split("/", 1)
    return (owner, name)

owner, name = parse_repo("facebook/react")
# owner = "facebook", name = "react"


# Split "key=value" into two parts
def parse_pair(pair: str) -> tuple[str, str]:
    key, value = pair.split("=", 1)
    return (key, value)

k, v = parse_pair("color=blue")
# k = "color", v = "blue"
print(k, v)