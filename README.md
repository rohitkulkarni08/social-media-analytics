# Social Media Analytics - Twitter Search Application

## Overview

The **Twitter Search Application** helps users efficiently search for tweets based on username, hashtags, or text content. This project leverages advanced data storage and retrieval techniques to ensure fast and scalable query execution. A caching mechanism is also implemented to optimize search performance.

## Features

- **User, Hashtag, and Keyword-Based Search**: Search for tweets using different parameters.
- **Trending Insights**: Retrieve the most popular users, tweets, and hashtags.
- **Efficient Data Storage**:
  - **MySQL** for structured user information.
  - **MongoDB** for dynamic and high-volume tweet data.
- **Caching Mechanism**: Uses a Python dictionary to store frequent queries, significantly reducing response times.

## System Architecture

- **Frontend**: Flask-based web application for user interaction.
- **Backend**:
  - MySQL: Stores user details.
  - MongoDB: Stores tweets and their metadata.
  - Python (Flask): Handles search queries and API requests.
- **Caching**: Speeds up repeated queries using a dictionary-based caching system.

## Dataset & Analysis

- Uses **COVID-19 Twitter datasets** from April 2020.
- **134,000 tweets** analyzed.
- **Sentiment Analysis** shows a slightly negative overall sentiment (0.02).
- **108,000 unique users** identified.
- Tweets classified by language, type, and engagement.

## Data Storage Strategy

- **User Data (MySQL)**:
  - Structured data with indexing for optimized query performance.
  - User attributes include ID, name, location, and verification status.
- **Tweet Data (MongoDB)**:
  - JSON-based storage for flexible schema handling.
  - Indexed on `User_Id` and `Text` for fast retrieval.

## Search Implementation

1. **User Search**: Enter `@username` to fetch details and recent tweets.
2. **Hashtag Search**: Enter `#hashtag` to retrieve top tweets using that hashtag.
3. **Keyword Search**: Search for specific words in tweets.
4. **Trending Insights**:
   - Top trending users based on engagement.
   - Most popular tweets ranked by retweets and likes.
   - Frequently used hashtags.

## Caching for Performance

- **LRU-based Cache**: Stores the most accessed queries.
- **500x Faster Queries**: Compared to direct database retrieval.
- **Persistent Storage**: Saves cache data for future use.

## Running the Application

### Prerequisites

- **Python 3.x**
- **MongoDB** & **MySQL**
- **Flask & Required Python Libraries**
- OpenAI API Key (if required for enhancements)

### Installation

```sh
# Clone the repository
git clone https://github.com/rohitkulkarni08/social-media-analytics.git
cd social-media-analytics

# Install dependencies
pip install -r requirements.txt

# Start the application
python app.py
