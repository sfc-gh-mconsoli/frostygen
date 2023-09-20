# FrostyGen - Random Data Generator
FrostyGen is a Streamlit application (SiS version available as well) designed to seamlessly generate random records and effortlessly integrate them into your Snowflake database. 

Test it out! https://frostygen.streamlit.app/

FrostyGen pushes data to Snowflake stages or tables, boosting your data preparation timelines and reducing the effort during your MVP/PoC first stages.
Connecting to your Snowflake instance using sidebar config is essential unless you're planning to save the generated data locally on your machine (Export to File).

![alt text](https://github.com/matteoconsoli92/frostygen/blob/main/screenshot.png?raw=true)

Do you want to run it on SiS (Streamlit in Snowflake)? Installing FrostyGen on your Snowflake account is very simple: 
1) Download "frosty_gen_sis.py" and "logo.png" from the GitHub repository. 
2) Create a new Streamlit app on your Snowflake account 
3) Paste the code into your new app. 
4) Upload the "logo.png" in the Streamlit application stage.
