import streamlit as st
import pandas as pd
import altair as alt
from AnalysisSpark import (
    get_genre_id,
    average_stats,
    genre_timeline,
    get_all_genre_names,
    get_top_10_movies,
    average_soundtrack_amount,
    top_composers)





st.set_page_config(page_title=" Genre Dashboard", layout="wide")
st.title("🎬 Movie Genre Popularity Dashboard")

# --- Genre Selection ---
genre_list = get_all_genre_names()
genre_name = st.selectbox("Select a Genre:", genre_list)

if genre_name:
    genre_id = get_genre_id(genre_name)

    if not genre_id:
        st.error(f"Genre '{genre_name}' not found.")
    else:
        # --- Fetch Data ---
        stats = average_stats(genre_id)
        timeline = genre_timeline(genre_id)
        top_movies = get_top_10_movies(genre_id)
        avg_soundtrack_len = average_soundtrack_amount(genre_id)
        top_composers_df = top_composers(genre_id)
        
        

        # --- Summary Stats ---
        if stats:
            st.subheader(f"Average Stats for '{genre_name}' Genre")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Avg Rating", f"{stats['avg_rating']:.2f}")
            col2.metric("Avg Vote Count", f"{stats['avg_vote_count']:.0f}")
            col3.metric("Avg Popularity", f"{stats['avg_popularity']:.2f}")
            col4.metric("Total Movies", stats["total_movies"])
        
        
        # --- Top 10 Movies ---
        if top_movies:
           st.subheader(f"Top 10 Highest Rated '{genre_name}' Movies")
           df_top = pd.DataFrame(top_movies)
           df_top = df_top.sort_values(by="vote_average", ascending=False)
           st.dataframe(df_top[["title", "vote_average", "release_date"]].reset_index(drop=True))
           
           
        # --- Average Sound Track Amount ---
        if avg_soundtrack_len:  
           st.subheader("Average Soundtrack Length")
           st.metric("Average Track Count", f"{avg_soundtrack_len:.2f} tracks")
        
        
        # --- Top 10 Composers ---
        if top_composers_df:
            st.subheader("Top 10 Most Frequent Composers")
            df_composers = pd.DataFrame(top_composers_df, columns=["Composer", "Count"])
            st.dataframe(df_composers)

        # --- Timeline Charts ---
        if timeline:
            df = timeline.toPandas()
            df.rename(columns={"release_year": "Year"}, inplace=True)
            df = df.sort_values("Year")

            st.subheader(f"Timeline of '{genre_name}' Genre")

            #  Rating Chart
            rating_chart = alt.Chart(df).mark_line(point=True).encode(
                x="Year:O",
                y=alt.Y("avg_rating:Q", title="Average Rating")
            ).properties(
                title="Average Rating Over Time", width=400, height=300
            )

            #  Popularity Chart
            popularity_chart = alt.Chart(df).mark_line(point=True).encode(
                x="Year:O",
                y=alt.Y("avg_popularity:Q", title="Average Popularity")
            ).properties(
                title="Average Popularity Over Time", width=400, height=300
            )

            # Movie Count Chart
            count_chart = alt.Chart(df).mark_line(point=True).encode(
                x="Year:O",
                y=alt.Y("movie_count:Q", title="Movie Count")
            ).properties(
                title="Number of Movies Released Over Time", width=400, height=300
            )

            col1, col2, col3 = st.columns(3)

            with col1:
                st.altair_chart(rating_chart, use_container_width=True)
            
            with col2:
                st.altair_chart(popularity_chart, use_container_width=True)
            
            with col3:
                st.altair_chart(count_chart, use_container_width=True)
