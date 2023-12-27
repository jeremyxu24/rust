from fuzzywuzzy import fuzz
import Levenshtein
from math import floor

def normalize_levenshtein(str1, str2):
    # Calculate the Levenshtein distance
    levenshtein_distance = Levenshtein.distance(str1, str2)
    
    # Calculate the maximum possible Levenshtein distance
    max_len_str1 = len(str1)
    max_len_str2 = len(str2)
    max_distance = max_len_str1 + max_len_str2
    
    # Calculate the normalized Levenshtein score
    normalized_score = 1 - (levenshtein_distance / max_distance)
    
    return normalized_score

def jaro_distance(s1, s2):
     
    # If the s are equal
    if (s1 == s2):
        return 1.0
 
    # Length of two s
    len1 = len(s1)
    len2 = len(s2)
 
    # Maximum distance upto which matching
    # is allowed
    max_dist = floor(max(len1, len2) / 2) - 1
 
    # Count of matches
    match = 0
 
    # Hash for matches
    hash_s1 = [0] * len(s1)
    hash_s2 = [0] * len(s2)
 
    # Traverse through the first
    for i in range(len1):
 
        # Check if there is any matches
        for j in range(max(0, i - max_dist), 
                       min(len2, i + max_dist + 1)):
             
            # If there is a match
            if (s1[i] == s2[j] and hash_s2[j] == 0):
                hash_s1[i] = 1
                hash_s2[j] = 1
                match += 1
                break
 
    # If there is no match
    if (match == 0):
        return 0.0
 
    # Number of transpositions
    t = 0
    point = 0
 
    # Count number of occurrences
    # where two characters match but
    # there is a third matched character
    # in between the indices
    for i in range(len1):
        if (hash_s1[i]):
 
            # Find the next matched character
            # in second
            while (hash_s2[point] == 0):
                point += 1
 
            if (s1[i] != s2[point]):
                t += 1
            point += 1
    t = t//2
 
    # Return the Jaro Similarity
    return (match/ len1 + match / len2 +
            (match - t) / match)/ 3.0

def normalize_jaro_winkler(str1, str2):
    # Calculate the Jaro-Winkler distance using your Jaro-Winkler function
    jaro_winkler_distance = jaro_distance(str1, str2)
    
    return jaro_winkler_distance

# Create a dictionary of name pairs
name_pairs = {
    "John": "Johnny",
    "Jonathan": "Jon",
    "Sam": "Sammy",
    "Samantha": "Sammie",
    "William": "Will",
    "Benjamin": "Ben",
    "Elizabeth": "Liz",
    "Katherine": "Kate",
    "Nicholas": "Nick",
    "Christopher": "Chris",
    "Jessica": "Jess",
    "Margaret": "Maggie",
    "Alexander": "Alex",
    "Jennifer": "Jenny",
    "Joseph": "Joe",
    "Michael": "Mike",
    "Michelle": "Micki",
    "Anthony": "Tony",
    "Kimberly": "Kim",
    "Daniel": "Dan"
}

# Print header
print("Name 1\tName 2\tFuzz Ratio\tJaro-Winkler\tLevenshtein")

# Iterate through the name pairs
for name1, name2 in name_pairs.items():
    # Calculate the fuzz ratio between the two strings
    fuzz_ratio = fuzz.ratio(name1, name2)
    
    # Calculate the Jaro-Winkler distance and normalize it
    jaro_winkler_distance = normalize_jaro_winkler(name1, name2)
    
    # Calculate the Levenshtein distance and normalize it
    levenshtein_distance = normalize_levenshtein(name1, name2)
    
    # Print the results in tabular format
    print(f"{name1}\t{name2}\t{fuzz_ratio}\t{jaro_winkler_distance:.4f}\t\t{levenshtein_distance:.4f}")
