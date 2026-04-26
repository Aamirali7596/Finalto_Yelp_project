# Entity Relationship Diagram
## Yelp Dataset — Source Data Model & Gold Star Schema

---

## Source ERD — 5 Raw Entities

```mermaid
erDiagram
  BUSINESS {
    string business_id PK
    string name
    string city
    string state
    double stars
    int review_count
    string categories
    boolean is_open
  }
  REVIEW {
    string review_id PK
    string business_id FK
    string user_id FK
    int stars
    timestamp date
    string text
  }
  USER {
    string user_id PK
    string name
    double average_stars
    string elite
    int fans
  }
  CHECKIN {
    string business_id FK
    string date
  }
  TIP {
    string business_id FK
    string user_id FK
    string text
    int compliment_count
  }
  BUSINESS ||--o{ REVIEW   : "receives"
  USER     ||--o{ REVIEW   : "writes"
  BUSINESS ||--o| CHECKIN  : "has"
  BUSINESS ||--o{ TIP      : "receives"
  USER     ||--o{ TIP      : "writes"
```

**Business is the root entity.** Every other entity references it via `business_id`. Review sits at the intersection of Business and User — it is the primary analytical fact.

---

## Gold Star Schema ERD

```mermaid
erDiagram
  FACT_REVIEWS {
    string review_id PK
    string business_id FK
    string user_id FK
    int date_id FK
    double review_stars
    int useful_votes
    int funny_votes
    int cool_votes
    int review_length_chars
  }
  FACT_CHECKINS {
    string checkin_id PK
    string business_id FK
    int date_id FK
    int checkin_hour
    string checkin_day_of_week
  }
  DIM_BUSINESS {
    string business_id PK
    string name
    string city
    string state
    double stars
    boolean is_open
    string primary_category
  }
  DIM_USER {
    string user_id PK
    string name
    double average_stars
    boolean is_elite
    int elite_years_count
    int fan_count
    int yelping_since_year
  }
  DIM_DATE {
    int date_id PK
    date full_date
    int year
    int month
    int quarter
    string day_name
    boolean is_weekend
  }
  FACT_REVIEWS  }o--|| DIM_BUSINESS : "about"
  FACT_REVIEWS  }o--|| DIM_USER     : "written by"
  FACT_REVIEWS  }o--|| DIM_DATE     : "on"
  FACT_CHECKINS }o--|| DIM_BUSINESS : "at"
  FACT_CHECKINS }o--|| DIM_DATE     : "on"
```

---

*Source ERD → raw Yelp JSON shape. Gold ERD → what the BI team queries.*