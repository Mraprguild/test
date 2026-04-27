from flask import Flask, render_template

app = Flask(__name__, template_folder="templates")

@app.route("/player/<media_type>/<encoded_url>")
def player(media_type, encoded_url):
    return render_template("player.html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
  
