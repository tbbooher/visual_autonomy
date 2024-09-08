from selenium import webdriver
from selenium.webdriver.common.by import By
import time

# this code doesn't work. it renders each svg, but not enough style is embedded in the svg to make it look like the original
# the original svg is rendered by the browser, and the browser applies the css to the svg
# the svg is not a static image, it is a dynamic image that is rendered by the browser
# need to address this in the future

# Set up the webdriver
driver = webdriver.Chrome()

# Navigate to your local server URL
driver.get('http://localhost:8000/sankey-diagram.html')

# Wait for the page to load completely
time.sleep(5)

# Find all the h2 elements (for themes)
themes = driver.find_elements(By.TAG_NAME, 'h2')

# Find all the SVG elements
svgs = driver.find_elements(By.TAG_NAME, 'svg')

# Ensure each theme has two SVGs (diagram and legend)
if len(themes) * 2 != len(svgs):
    print("Error: The number of SVGs and themes do not match the expected 2 SVGs per theme!")
else:
    # Loop through each theme and its corresponding two SVGs
    for i, theme in enumerate(themes):
        # Get the theme text and sanitize it for a filename
        theme_text = theme.text.replace('Sankey Diagram for Theme: ', '').replace(' ', '_').replace(':', '')

        # Save the first SVG (diagram)
        diagram_svg = svgs[i * 2]
        diagram_filename = f"sankey_diagram_{theme_text}.svg"
        with open(diagram_filename, "w") as file:
            file.write(diagram_svg.get_attribute('outerHTML'))
        print(f"Saved: {diagram_filename}")

        # Save the second SVG (legend)
        legend_svg = svgs[i * 2 + 1]
        legend_filename = f"sankey_legend_{theme_text}.svg"
        with open(legend_filename, "w") as file:
            file.write(legend_svg.get_attribute('outerHTML'))
        print(f"Saved: {legend_filename}")

# Close the browser
driver.quit()
