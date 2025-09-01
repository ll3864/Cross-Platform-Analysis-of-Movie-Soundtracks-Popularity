import streamlit as st
import pandas as pd
import altair as alt
import matplotlib.pyplot as plt
import seaborn as sns
from DemoAnalysis import (
    get_genre_id,
    average_stats,
    genre_timeline,
    get_all_genre_names,
    get_top_10_movies,
    average_soundtrack_amount,
    get_top_composers,
)

st.set_page_config(page_title="🎬 Genre Dashboard", layout="wide")
st.title("🎵 Movie & Soundtrack Genre Analysis Dashboard")

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
        top_composers_df = get_top_composers(genre_id)

        # --- Summary Stats ---
        if stats:
            st.subheader(f"Average Stats for '{genre_name}' Genre")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Avg Rating", f"{stats['Movie_Avg_Rating']:.2f}")
            col2.metric("Avg Vote Count", f"{stats['Movie_Avg_VoteCount']:.0f}")
            col3.metric("Avg Popularity", f"{stats['Movie_Avg_Popularity']:.2f}")
            col4.metric("Total Movies", stats["Total_Movie_Counts"])

        # --- Timeline Charts ---
        if timeline and timeline.limit(1).count() > 0:
            df = timeline.toPandas()
            df.rename(columns={"release_year": "Year"}, inplace=True)
            df = df.sort_values("Year")

            df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
            df = df.dropna()

            st.subheader(f"Timeline of '{genre_name}' Genre")

            rating_chart = alt.Chart(df).mark_line(point=True).encode(
                x="Year:O",
                y=alt.Y("avg_rating:Q", title="Average Rating")
            ).properties(
                title="Average Rating Over Time", width=400, height=300
            )

            popularity_chart = alt.Chart(df).mark_line(point=True).encode(
                x="Year:O",
                y=alt.Y("avg_popularity:Q", title="Average Popularity")
            ).properties(
                title="Average Popularity Over Time", width=400, height=300
            )

            count_chart = alt.Chart(df).mark_line(point=True).encode(
                x="Year:O",
                y=alt.Y("movie_count:Q", title="Movie Count")
            ).properties(
                title="Number of Movies Released Over Time", width=400, height=300
            )

            col1, col2, col3 = st.columns(3)
            with col1: st.altair_chart(rating_chart, use_container_width=True)
            with col2: st.altair_chart(popularity_chart, use_container_width=True)
            with col3: st.altair_chart(count_chart, use_container_width=True)

        # --- Top 10 Movies ---
        if top_movies:
            st.subheader(f"Top 10 Highest Rated '{genre_name}' Movies")
            df_top = pd.DataFrame(top_movies)
            st.dataframe(df_top.reset_index(drop=True))

            # Correlation Plot
            st.subheader("Correlation Between Movie Rating and Soundtrack Popularity")
            fig, ax = plt.subplots(figsize=(6, 4))
            sns.regplot(data=df_top, x="vote_average", y="Soundtrack_Popularity", ax=ax)
            ax.set_xlabel("Movie Rating")
            ax.set_ylabel("Soundtrack Popularity")
            ax.set_title("Correlation between Rating and Soundtrack Popularity")
            st.pyplot(fig)

            if df_top["vote_average"].std() > 0 and df_top["Soundtrack_Popularity"].std() > 0:
                corr = df_top["vote_average"].corr(df_top["Soundtrack_Popularity"])
                st.markdown(f"**Correlation Coefficient**: {corr:.3f}")
            else:
                st.markdown("**Correlation Coefficient**: Not enough variance to compute.")

        # --- Average Sound Track Amount ---
        if avg_soundtrack_len:
            st.subheader("Average Soundtrack Length")
            st.metric("Average Track Count", f"{avg_soundtrack_len:.2f} tracks")

        # --- Top Composers ---
        if top_composers_df:
            st.subheader("Top 5 Most Frequent Composers")
            df_composers = pd.DataFrame(top_composers_df, columns=["Composer", "Count"])
            st.dataframe(df_composers)
