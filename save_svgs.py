from selenium import webdriver
import os

# Set up the webdriver
driver = webdriver.Chrome()

# Navigate to your local server URL
driver.get('http://localhost:8000/sankey-diagram.html')

# Wait for the SVG to render
driver.implicitly_wait(10)

# Find all the h2 elements (for themes)
themes = driver.find_elements_by_tag_name('h2')

# Find all the SVG elements
svgs = driver.find_elements_by_tag_name('svg')

# Ensure we have the same number of headings and SVGs
if len(themes) != len(svgs):
    print("Error: The number of SVGs and themes do not match!")
else:
    # Loop through each theme and its corresponding SVG
    for i, (theme, svg) in enumerate(zip(themes, svgs)):
        # Get the theme text and sanitize it for a filename
        theme_text = theme.text.replace('Sankey Diagram for Theme: ', '').replace(' ', '_').replace(':', '')

        # Create the filename
        filename = f"sankey_diagram_{theme_text}.svg"
        
        # Save the SVG content
        with open(filename, "w") as file:
            file.write(svg.get_attribute('outerHTML'))
        print(f"Saved: {filename}")

# Close the browser
driver.quit()
