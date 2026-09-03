from supabase import create_client, Client

SUPABASE_URL = "https://vdfvwheweyzuafbxggir.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZkZnZ3aGV3ZXl6dWFmYnhnZ2lyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODgyODc2NTksImV4cCI6MjEwMzg2MzY1OX0.-lIVVCfQXmSYQu3edKaCNYsLN7DYOGaj-FYAVKZF2pg"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)