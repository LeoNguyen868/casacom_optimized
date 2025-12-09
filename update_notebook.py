import json
import os

notebook_path = "verify_pipeline.ipynb"

def update_notebook():
    if not os.path.exists(notebook_path):
        print(f"Notebook {notebook_path} not found.")
        return

    with open(notebook_path, 'r') as f:
        nb = json.load(f)

    # Visualization Code to Append
    viz_source = [
        "\n",
        "# 4. Visualizing Score Distributions\n",
        "import seaborn as sns\n",
        "import matplotlib.pyplot as plt\n",
        "\n",
        "def plot_distributions():\n",
        "    print('Fetching score data from ClickHouse...')\n",
        "    query = \"\"\"\n",
        "    SELECT \n",
        "        geohash, \n",
        "        home_score, \n",
        "        work_score, \n",
        "        leisure_score, \n",
        "        pingsink_score\n",
        "    FROM view_aggregated_data \n",
        "    ARRAY JOIN geohash, home_score, work_score, leisure_score, pingsink_score\n",
        "    \"\"\"\n",
        "    \n",
        "    # Re-use run_query from previous cells or redefine if needed\n",
        "    # Assuming run_query is available in notebook state\n",
        "    data = run_query(query)\n",
        "    if not data or 'data' not in data:\n",
        "        print('No data fetched.')\n",
        "        return\n",
        "\n",
        "    df_scores = pd.DataFrame(data['data'])\n",
        "    print(f'Fetched {len(df_scores)} records.')\n",
        "    \n",
        "    score_cols = ['home_score', 'work_score', 'leisure_score', 'pingsink_score']\n",
        "    for col in score_cols:\n",
        "        df_scores[col] = pd.to_numeric(df_scores[col], errors='coerce')\n",
        "\n",
        "    sns.set_theme(style='whitegrid')\n",
        "    fig, axes = plt.subplots(2, 2, figsize=(14, 10))\n",
        "    fig.suptitle('Score Distributions (ClickHouse Output)', fontsize=16)\n",
        "\n",
        "    sns.histplot(df_scores['home_score'], bins=20, kde=True, ax=axes[0, 0], color='skyblue')\n",
        "    axes[0, 0].set_title('Home Score')\n",
        "\n",
        "    sns.histplot(df_scores['work_score'], bins=20, kde=True, ax=axes[0, 1], color='orange')\n",
        "    axes[0, 1].set_title('Work Score')\n",
        "\n",
        "    sns.histplot(df_scores['leisure_score'], bins=20, kde=True, ax=axes[1, 0], color='green')\n",
        "    axes[1, 0].set_title('Leisure Score')\n",
        "\n",
        "    sns.histplot(df_scores['pingsink_score'], bins=20, kde=True, ax=axes[1, 1], color='red')\n",
        "    axes[1, 1].set_title('Pingsink Score')\n",
        "\n",
        "    plt.tight_layout()\n",
        "    plt.show()\n",
        "\n",
        "plot_distributions()\n"
    ]

    new_cell = {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": viz_source
    }

    # Remove existing cells with this logic to avoid duplication if run multiple times?
    # For now just append.
    nb['cells'].append(new_cell)

    with open(notebook_path, 'w') as f:
        json.dump(nb, f, indent=1)
    
    print("Notebook updated.")

if __name__ == "__main__":
    update_notebook()
