import csv
import re
from traceback import print_stack
from pyparsing import Regex
import redis
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query


class Redis_Client():
    redis = None
    
    def __init__(self):
        self.redis = None
    

    def connect(self):
        try:
            self.redis = redis.Redis(
                host='redis-10265.c98.us-east-1-4.ec2.redns.redis-cloud.com',
                port=10265,
                db=0,
                username='default',  # Usually 'default' for Redis Cloud
                password='VfBcRzsR9VFhrFX3jHJs9XfIm3WmME7d',  # Replace with your actual password
                decode_responses=True,
                socket_connect_timeout=5
            )
            # Test connection
            self.redis.ping()
            print("Successfully connected to Redis!")
            print(f"Redis Info: {self.redis.info('server')['redis_version']}")
        except Exception as e:
            print(f"Error connecting to Redis: {e}")
            print_stack()
    
    """
    Load the users dataset into Redis DB.
    """
    def load_users(self, file):
        result = 0
        try:
            with open(file, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()
                f.seek(0)
                
                if first_line.startswith('"user:'):
                    print("Detected Redis protocol format")
                    return self._load_users_redis_format(file)
                else:
                    print("Detected CSV format")
                    return self._load_users_csv_format(file)
                    
        except FileNotFoundError:
            print(f"Error: File '{file}' not found")
            return 0
        except Exception as e:
            print(f"Error loading users: {e}")
            print_stack()
            return 0
    
    def _load_users_redis_format(self, file):
        """Load users from Redis protocol format"""
        result = 0
        try:
            with open(file, 'r', encoding='utf-8') as f:
                pipe = self.redis.pipeline()
                
                for line in f:
                    # Parse the line: "user:1" "first_name" "Mohammed" "last_name" "Ahern" ...
                    parts = line.strip().split('" "')
                    
                    if len(parts) < 3:
                        continue
                    
                    # Clean up quotes
                    parts = [p.strip('"') for p in parts]
                    
                    key = parts[0]  # user:1
                    
                    # Create a dictionary from field-value pairs
                    user_data = {}
                    for i in range(1, len(parts) - 1, 2):
                        field = parts[i]
                        value = parts[i + 1]
                        user_data[field] = value
                    
                    # Store in Redis
                    pipe.hset(key, mapping=user_data)
                    result += 1
                
                pipe.execute()
                print(f"Loaded {result} users into Redis")
                
        except Exception as e:
            print(f"Error loading users from Redis format: {e}")
            print_stack()
        
        return result
    
    def _load_users_csv_format(self, file):
        """Load users from CSV format"""
        result = 0
        try:
            with open(file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                pipe = self.redis.pipeline()
                
                for row in reader:
                    user_id = row['id']
                    key = f"user:{user_id}"
                    
                    # Store user data as hash
                    pipe.hset(key, mapping=row)
                    result += 1
                
                pipe.execute()
                print(f"Loaded {result} users into Redis")
                
        except Exception as e:
            print(f"Error loading users from CSV: {e}")
            print_stack()
        
        return result
    
    """
    Load the scores dataset into Redis DB.
    
    """
    def load_scores(self, file='datasets/userscores.csv'):
        try:
            pipe = self.redis.pipeline()
            result_count = 0
            
            with open(file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    # Extract user_id from "user:1" format
                    user_key = row['user:id']
                    user_id = user_key.split(':')[1] if ':' in user_key else user_key
                    
                    leaderboard_id = row['leaderboard']
                    score = float(row['score'])
                    
                    # Add to sorted set (leaderboard)
                    key = f"leaderboard:{leaderboard_id}"
                    pipe.zadd(key, {user_id: score})
                    result_count += 1
            
            result = pipe.execute()
            print(f"Loaded {result_count} scores into Redis leaderboards")
            return result
            
        except FileNotFoundError:
            print(f"Error: File '{file}' not found")
            return []
        except Exception as e:
            print(f"Error loading scores: {e}")
            print_stack()
            return []
    
    """
    Return all the attributes of the user by usr (user id)
    Uses: HGETALL command to get all fields from hash
    """
    def query1(self, usr):
        print(f"\n{'='*60}")
        print("Executing query 1: Get all attributes of user")
        print(f"{'='*60}")
        
        try:
            key = f"user:{usr}"
            result = self.redis.hgetall(key)
            
            if result:
                print(f"\nUser ID {usr} - All Attributes:")
                print("-" * 60)
                for field, value in result.items():
                    print(f"{field:15}: {value}")
            else:
                print(f"No user found with ID: {usr}")
            
            return result
        except Exception as e:
            print(f"Error in query1: {e}")
            print_stack()
            return None
    
    """
    Return the coordinate (longitude and latitude) of the user by the usr.
    Uses: HMGET command to get specific fields from hash
    """
    def query2(self, usr):
        print(f"\n{'='*60}")
        print("Executing query 2: Get user coordinates")
        print(f"{'='*60}")
        
        try:
            key = f"user:{usr}"
            coordinates = self.redis.hmget(key, ['longitude', 'latitude'])
            
            if coordinates[0] and coordinates[1]:
                result = {
                    'longitude': coordinates[0],
                    'latitude': coordinates[1]
                }
                print(f"\nUser ID {usr} - Coordinates:")
                print("-" * 60)
                print(f"Longitude: {result['longitude']}")
                print(f"Latitude:  {result['latitude']}")
            else:
                print(f"No coordinates found for user ID: {usr}")
                result = None
            
            return result
        except Exception as e:
            print(f"Error in query2: {e}")
            print_stack()
            return None
    
    """
    Get the keys and last names of the users whose ids do not start with an odd number.
    Uses: SCAN command starting from cursor 1280
    """
    def query3(self):
        print(f"\n{'='*60}")
        print("Executing query 3: Users with IDs NOT starting with odd number")
        print(f"{'='*60}")
        
        try:
            cursor = 1280
            userids = []
            result_lastnames = []
            count = 10  # Number of elements to scan per call
            
            # Scan through keyspace starting at cursor 1280
            while True:
                cursor, keys = self.redis.scan(cursor=cursor, match='user:*', count=count)
                
                for key in keys:
                    # Extract user_id from key (user:123 -> 123)
                    user_id = key.split(':')[1]
                    
                    # Check if first digit is NOT odd (i.e., it's even: 0,2,4,6,8)
                    first_digit = int(user_id[0])
                    if first_digit % 2 == 0:  # Even number (not odd)
                        last_name = self.redis.hget(key, 'last_name')
                        if last_name:
                            userids.append(key)
                            result_lastnames.append(last_name)
                
                # Stop if we've returned to cursor 0 or found enough results
                if cursor == 0 or len(userids) >= 20:
                    break
            
            print(f"\nFound {len(userids)} users with IDs NOT starting with odd number:")
            print("-" * 60)
            for i, (uid, lastname) in enumerate(zip(userids[:10], result_lastnames[:10]), 1):
                print(f"{i}. Key: {uid:20} | Last Name: {lastname}")
            
            if len(userids) > 10:
                print(f"... and {len(userids) - 10} more")
            
            return userids, result_lastnames
        except Exception as e:
            print(f"Error in query3: {e}")
            print_stack()
            return [], []
    
    """
    Return the females in China or Russia with latitude between 40 and 46.
    Uses: RediSearch with secondary index (if available), otherwise manual filtering
    """
    def query4(self):
        print(f"\n{'='*60}")
        print("Executing query 4: Female users in China/Russia with lat 40-46")
        print(f"{'='*60}")
        
        # if not REDISEARCH_AVAILABLE:
        #     # Manual filtering approach
        #     print("Using manual filtering (RediSearch not available)")
        #     return self._query4_manual()
        
        try:
            # Create index if it doesn't exist
            index_name = "idx:users"
            
            try:
                # Try to create index
                self.redis.ft(index_name).create_index([
                    TextField("first_name"),
                    TextField("gender"),
                    TagField("country"),
                    NumericField("latitude")
                ], definition=IndexDefinition(prefix=["user:"]))
                print("Created search index")
            except Exception as e:
                if "Index already exists" in str(e):
                    print("Search index already exists")
                else:
                    raise e
            
            # Build query: gender=female AND (country=China OR country=Russia) AND latitude:[40 46]
            query_str = "@gender:female (@country:{China|Russia}) @latitude:[40 46]"
            query = Query(query_str).return_fields('id', 'first_name', 'last_name', 'country', 'latitude')
            
            # Execute search
            result = self.redis.ft(index_name).search(query)
            
            print(f"\nFound {result.total} matching users:")
            print("-" * 60)
            
            if result.docs:
                for i, doc in enumerate(result.docs, 1):
                    print(f"{i}. ID: {doc.id.split(':')[1]:6} | "
                          f"Name: {doc.first_name} {doc.last_name:15} | "
                          f"Country: {doc.country:10} | "
                          f"Lat: {doc.latitude}")
            else:
                print("No matching users found")
            
            return result
        except Exception as e:
            print(f"Error in query4 with RediSearch: {e}")
            print("Falling back to manual filtering")
            return self._query4_manual()
    
    def _query4_manual(self):
        """Manual filtering for query4 when RediSearch is not available"""
        try:
            matching_users = []
            cursor = 0
            
            while True:
                cursor, keys = self.redis.scan(cursor=cursor, match='user:*', count=100)
                
                for key in keys:
                    user_data = self.redis.hgetall(key)
                    
                    if user_data:
                        gender = user_data.get('gender', '').lower()
                        country = user_data.get('country', '')
                        latitude = float(user_data.get('latitude', 0))
                        
                        # Check conditions: female AND (China OR Russia) AND latitude between 40 and 46
                        if (gender == 'female' and 
                            country in ['China', 'Russia'] and 
                            40 <= latitude <= 46):
                            matching_users.append({
                                'id': user_data.get('id'),
                                'first_name': user_data.get('first_name'),
                                'last_name': user_data.get('last_name'),
                                'country': country,
                                'latitude': latitude
                            })
                
                if cursor == 0:
                    break
            
            print(f"\nFound {len(matching_users)} matching users:")
            print("-" * 60)
            
            for i, user in enumerate(matching_users, 1):
                print(f"{i}. ID: {user['id']:6} | "
                      f"Name: {user['first_name']} {user['last_name']:15} | "
                      f"Country: {user['country']:10} | "
                      f"Lat: {user['latitude']}")
            
            return matching_users
        except Exception as e:
            print(f"Error in manual query4: {e}")
            print_stack()
            return []
    
    """
    Get the email ids of the top 10 players (in terms of score) in leaderboard:2
    Uses: ZREVRANGE command with WITHSCORES option on sorted set
    """
    def query5(self):
        print(f"\n{'='*60}")
        print("Executing query 5: Top 10 players in leaderboard:2")
        print(f"{'='*60}")
        
        try:
            # Get top 10 users from leaderboard:2 (highest scores first)
            leaderboard_key = "leaderboard:2"
            top_users = self.redis.zrevrange(leaderboard_key, 0, 9, withscores=True)
            
            result_emails = []
            
            print(f"\nTop 10 Players in Leaderboard 2:")
            print("-" * 60)
            print(f"{'Rank':<6} {'User ID':<10} {'Score':<12} {'Email'}")
            print("-" * 60)
            
            for rank, (user_id, score) in enumerate(top_users, 1):
                # Get email from user hash
                user_key = f"user:{user_id}"
                email = self.redis.hget(user_key, 'email')
                
                if email:
                    result_emails.append(email)
                    print(f"{rank:<6} {user_id:<10} {score:<12.2f} {email}")
                else:
                    print(f"{rank:<6} {user_id:<10} {score:<12.2f} [Email not found]")
            
            return result_emails
        except Exception as e:
            print(f"Error in query5: {e}")
            print_stack()
            return []


# Main execution
if __name__ == "__main__":
    rs = Redis_Client()
    rs.connect()
    
    # Load data (uncomment after setting up files)
    rs.load_users("datasets/users.txt")
    rs.load_scores("datasets/userscores.csv")
    
    # Execute queries
    rs.query1(299)
    rs.query2(2836)
    rs.query3()
    rs.query4()

    rs.query5()
