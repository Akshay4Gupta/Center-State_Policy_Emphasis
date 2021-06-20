top5Schemes = {
	'industrialization': ['North East policy', 'Technology Upgradation Fund Scheme', 'Infrastructure Development Programme', 'Standup India', 'Textile Industry'],
	'environment': ['National Action Plan on Climate Change', 'National Clean Air Programme', 'Namami Gange Programme', 'National Green Corps', 'National Initiative on Climate Resilient Agriculture'],
	'humanDevelopment': ['MGNREGA', 'PDS & Food Security', 'Awas Yojana', 'Jnnurm', 'Jan Dhan Yojana', 'Pension Schemes'],
	'tourism_culture': ['Swadesh Darshan scheme', 'Cultural Heritage', 'Prasad scheme', 'Transport schemes', 'Project mausam'],
	'agriculture': ['National Mission For Sustainable Agriculture', 'Fasal Bima Yojana', 'Soil Health Card', 'Kisan Credit Card', 'PM-KISAN'],
	'health_hygiene': ['National Health Mission', 'Health Insurance', 'Clean India Mission', 'Child Development Services', 'Swajal Yojana', 'Janani Suraksha Yojana']
}

newsSource = ['The Times Of India', 'Hindustan Times', 'The New Indian Express', 'The Hindu', 'Indian Express']

inp = 'input/'
out = 'output/'
path = 'schemes/'
task = ['scheme_state_party', 'state', 'print_state_articles_count', 'scheme', 'sentiment']

large_states = ['andhra pradesh',
  'kerala',
  'maharashtra',
  'odisha',
  'rajasthan',
  'tamil nadu',
  'uttar pradesh',
  'madhya pradesh',
  'nct of delhi']

medium_states = ['assam',
  'bihar',
  'chhattisgarh',
  'gujarat',
  'haryana',
  'himachal pradesh',
  'jammu & kashmir',
  'jharkhand',
  'ladakh',
  'karnataka',
  'punjab',
  'west bengal']

small_states = ['arunachal pradesh',
  'goa',
  'manipur',
  'meghalaya',
  'mizoram',
  'nagaland',
  'puducherry',
  'sikkim',
  'tripura',
  'uttarakhand']


state_abbr = {'andaman & nicobar islands': 'AN', 
'andhra pradesh': 'AP', 
'arunachal pradesh': 'AR', 
'assam': 'AS', 
'bihar': 'BR', 
'chandigarh': 'CH', 
'chhattisgarh'	: 'CT', 
'dadra & nagar haveli and daman & diu': 'DNDD',
'nct of delhi': 'DL', 
'goa': 'GA', 
'gujarat': 'GJ', 
'haryana'	: 'HR', 
'himachal pradesh': 'HP',
'ladakh': 'LD', 
'jammu & kashmir': 'JK', 
'jharkhand': 'JH', 
'karnataka': 'KA', 
'kerala': 'KL', 
'lakshadweep': 'LD', 
'madhya pradesh': 'MP', 
'maharashtra': 'MH', 
'manipur': 'MN', 
'meghalaya'	: 'ML', 
'mizoram': 'MZ', 
'nagaland': 'NL', 
'odisha': 'OR', 
'puducherry': 'PY', 
'punjab': 'PB', 
'rajasthan': 'RJ', 
'sikkim': 'SK', 
'tamil nadu': 'TN', 
'telangana': 'TG', 
'tripura'	: 'TR', 
'uttar pradesh': 'UP', 
'uttarakhand': 'UT', 
'west bengal': 'WB'}
