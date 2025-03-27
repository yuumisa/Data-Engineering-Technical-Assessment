# Data Engineering Technical Assessment

For data security reasons, only the transformation scripts will be available here.
 
During the exercise, there were other recommendations and questions that I would've discussed with the client. Below I will list these out in no particular order:

1. There were missing years in Business Service Pipeline file. I would recommend a placeholder value until this can be thoroughly discussed with the client.
2. There were some deals where there was no status. Should those be given active, dead, or some other option?
3. The creation of the load files were pretty ad hoc for this exercise. If given the chance, I would like to sit down with both sides of the business and create a unified template for every table in DealCloud.
4. There were a few tables I would recommend creating in the future: Projects, MD/Coverage Personnel, and Event details (address, budget, etc.). This way each can have their own unique identifiers.
5. I would recommend meeting with the client to create/modify choice fields. Some of these include: Status, Portfolio Company, Active stage, etc.
6. The creation of the contacts and company tables were arbitrary on what should go in. Banks and Bankers should also be included in the future.

Other Notes:

I did the work in Jupyter notebooks for easier data exploration/analysis but I would export these to .py scripts for actual work (Scripts in the folder may not work out of the box).
I exported the files into Excel and CSV files. Excel files for easier viewing/manipulation when working with clients and csv (or really any other file format) for loading.
There is still a lot more that could be done with client input, more data, and general cleanup.
