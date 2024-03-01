#
#
def filter_cond(line_dict):
    """Filter function
    Takes a dict with field names as argument
    Returns True if conditions are satisfied
    """
    if line_dict["if1"].isdigit():
      if (20 < int(line_dict["if1"]) < 40):
         return True
      else:
         return False
    else:
      return False

