import pandas as pd
from tournament_extractor import ChessTournamentExtractor

tournament_id = ['vii-arena-loggichess-1171620/1',
                 'vii-arena-loggichess-1171621/1',
                 'vii-arena-loggichess-1171622/1',
                 'vii-arena-loggichess-1171623/1']
    
# Create an instance of ChessTournamentExtractor
extractor = ChessTournamentExtractor(tournament_id)
    
# Extract data using the extractor
extracted_data = extractor.process_tournaments()

# Save data as a csv file
extracted_data.to_csv('processed_tournament_data.csv', index=False)
