// Description: Create a Sankey diagram using D3.js
// Author: Tim Booher and Emily 

document.addEventListener("DOMContentLoaded", function () {
  console.log("DOM loaded");
  try {
    if (typeof d3 === "undefined") throw new Error("D3 library is not loaded");
    if (typeof d3.sankey === "undefined")
      throw new Error("D3 Sankey plugin is not loaded");

    // Load data from an external JSON file
    d3.json("flow_data.json")
      .then(function (data) {
        // Group data by unique source themes
        const themes = Array.from(new Set(data.map((d) => d.source_theme)));

        themes.forEach((theme) => {
          let margin = { top: 10, right: 10, bottom: 10, left: 10 };
          let width = 1100 - margin.left - margin.right;
          const height = 600 - margin.top - margin.bottom;

          // Filter data for the current theme
          const themeData = data.filter((d) => d.source_theme === theme);

          // Create a container for each Sankey diagram
          const container = d3
            .select("#chart")
            .append("div")
            .attr("class", "theme-container");

          // Add a title for the theme
          container.append("h2").text(`Sankey Diagram for Theme: ${theme}`);

          // Create an SVG for the Sankey diagram
          const svg = container
            .append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
            .call(
              d3.zoom().on("zoom", function (event) {
                svg.attr("transform", event.transform);
              })
            )
            .append("g")
            .attr("transform", `translate(${margin.left},${margin.top})`);

          // Create a tooltip div that is hidden by default
          const tooltip = d3.select("body").append("div")
            .attr("class", "d3-tooltip")
            .style("opacity", 0);

          // Create a color scale for the nodes
          const targetColorScale = d3.scaleOrdinal(d3.schemeTableau10);

          // Create a set of all unique node names for the current theme
          const nodeNames = new Set();
          themeData.forEach((d) => {
            nodeNames.add(d.source);
            nodeNames.add(d.target);
          });

          // Create an array of node objects with funding information
          const nodes = Array.from(nodeNames).map((name) => {
            const themeNodeData = themeData.filter((d) => d.source === name || d.target === name);
            const nodeData = themeNodeData[0];
            return {
              name: name,
              fundingIn: 0,
              fundingOut: 0,
              targetFunding: 0, // Add targetFunding to handle final node case
              source_companies: nodeData ? nodeData.source_companies : "no data",
              source_description: nodeData ? nodeData.source_description : "no data",
              source_name: nodeData ? nodeData.source_name : "no data",
              source_org: nodeData ? nodeData.source_org : "no data",
            };
          });

          // Calculate total funding for each node
          themeData.forEach((d) => {
            const sourceNode = nodes.find((n) => n.name === d.source);
            const targetNode = nodes.find((n) => n.name === d.target);
            if (sourceNode) sourceNode.fundingOut += d.value;
            if (targetNode) {
              targetNode.fundingIn += d.value;
              targetNode.targetFunding = d.target_funding; // Assign targetFunding for final node
            }
          });

          // Create a map of node names to indices
          const nodeMap = new Map(
            nodes.map((node, index) => [node.name, index])
          );

          // Map the links to use node indices instead of names
          const links = themeData.map((d) => ({
            source: nodeMap.get(d.source),
            target: nodeMap.get(d.target),
            value: d.value,
            source_funding: d.source_funding || 0,
            target_funding: d.target_funding || 0,
            source_description: d.source_description || "no data",
            source_companies: d.source_companies || "no data",
            source_name: d.source_name || "no data",
            source_org: d.source_org || "no data",
          }));

          // Create a map of source nodes to their target nodes
          const sourceToTargetMap = new Map();
          links.forEach((link) => {
            if (!sourceToTargetMap.has(link.source)) {
              sourceToTargetMap.set(link.source, []);
            }
            sourceToTargetMap.get(link.source).push(link.target);
          });

          const sankey = d3
            .sankey()
            .nodeWidth(30)
            .nodePadding(10)
            .extent([
              [1, 1],
              [width - 150, height - 6],
            ])
            .nodeSort((a, b) => {
              const aTargets = sourceToTargetMap.get(a.index) || [];
              const bTargets = sourceToTargetMap.get(b.index) || [];
              if (aTargets.length === 0 && bTargets.length === 0) return 0;
              if (aTargets.length === 0) return 1;
              if (bTargets.length === 0) return -1;
              return d3.ascending(aTargets[0], bTargets[0]);
            })
            .nodeId((d) => d.index);

          const graph = sankey({
            nodes: nodes.map((d) => Object.assign({}, d)), // Ensure we pass a copy
            links: links.map((d) => Object.assign({}, d)), // Ensure we pass a copy
          });

          sankey.nodeSort(null); // disable nodeSort to enable draggable nodes

          // Create a map to store the base color for each target node
          const targetColors = new Map();
          graph.nodes.forEach((node) => {
            if (node.fundingIn > 0 && !targetColors.has(node.name)) {
              targetColors.set(node.name, targetColorScale(node.name));
            }
          });

          // Check if the node is a final node (no outgoing links)
          function isFinalNode(d) {
            return graph.links.every((link) => link.source.index !== d.index);
          }

          const link = svg
            .append("g")
            .selectAll(".link")
            .data(graph.links)
            .enter()
            .append("path")
            .attr("class", "link")
            .attr("d", d3.sankeyLinkHorizontal())
            .style("stroke-width", (d) => Math.max(1, d.width))
            .on("mouseover", function (event, d) {
              tooltip.transition()
                .duration(200)
                .style("opacity", .9);
              tooltip.html(`<strong>${d.source.source_name} → ${d.target.name}</strong><br>` +
                'Description: ' + d.source_description + '<br>' +
                'Companies: ' + d.source_companies + '<br>' +
                `Value: $${d.value.toFixed(2)}M<br>` +
                `Source Funding: $${d.source_funding.toFixed(2)}M<br>` +
                `Source Org: ${d.source_org}<br>` +
                `Target Funding: $${d.target_funding.toFixed(2)}M`)
                .style("left", (event.pageX) + "px")
                .style("top", (event.pageY - 28) + "px");
            })
            .on("mouseout", function () {
              tooltip.transition()
                .duration(500)
                .style("opacity", 0);
            });


          const node = svg
            .append("g")
            .selectAll(".node")
            .data(graph.nodes)
            .enter()
            .append("g")
            .attr("class", "node")
            .attr("transform", (d) => `translate(${d.x0},${d.y0})`)
            .call(
              d3.drag()
                .subject(function (d) {
                  return d;
                })
                .on("start", function () {
                  this.parentNode.appendChild(this);
                })
                .on("drag", dragmove)
            )
            .on("mouseover", function (event, d) {
              tooltip.transition()
                .duration(200)
                .style("opacity", .9);
              tooltip.html(`<strong>${d.name}</strong><br>` +
                `Total Funding In: $${d.fundingIn.toFixed(2)}M<br>` +
                `Total Funding Out: $${d.fundingOut.toFixed(2)}M<br>` +
                `Target Funding: $${d.targetFunding.toFixed(2)}M`)
                .style("left", (event.pageX) + "px")
                .style("top", (event.pageY - 28) + "px");
            })
            .on("mouseout", function (d) {
              tooltip.transition()
                .duration(500)
                .style("opacity", 0);
            });

          // Create the in-bar with adjusted height
          node
            .append("rect")
            .attr("class", "in-bar")
            .attr("x", 0)
            .attr("height", (d) => {
              const baseHeight = d.y1 - d.y0;
              if (d.fundingOut > d.fundingIn) {
                return (d.fundingIn / d.fundingOut) * baseHeight;
              }
              return baseHeight;
            })
            .attr("width", sankey.nodeWidth() / 2)
            .attr("fill", (d) => {
              let targetNode;
              if (d.sourceLinks.length > 0) {
                targetNode = d.sourceLinks[0].target;
              } else {
                targetNode = d;
              }
              return targetColors.get(targetNode.name);
            })
            .attr("opacity", 0.7);

          // Create the out-bar or target-funding bar for final nodes
          node
            .append("rect")
            .attr("class", "out-bar")
            .attr("x", sankey.nodeWidth() / 2)
            .attr("height", (d) => {
              const baseHeight = d.y1 - d.y0;
              if (isFinalNode(d)) {
                return 0;
              } else if (d.fundingOut > d.fundingIn) {
                return baseHeight;
              } else {
                return (d.fundingOut / d.fundingIn) * baseHeight;
              }
            })
            .attr("width", sankey.nodeWidth() / 2)
            .attr("fill", (d) => {
              let targetNode;
              if (d.sourceLinks.length > 0) {
                targetNode = d.sourceLinks[0].target;
              } else {
                targetNode = d;
              }
              const targetColor = targetColors.get(targetNode.name);
              return d3.color(targetColor).darker(0.5);
            })
            .attr("opacity", 0.7);

          // Create the wrapped bar segments for the final node
          node
            .filter((d) => isFinalNode(d))
            .each(function (d) {
              const baseHeight = d.y1 - d.y0;
              const totalHeight = (d.targetFunding / d.fundingIn) * baseHeight;
              const numSegments = Math.ceil(totalHeight / baseHeight);
              const segmentHeight = baseHeight;

              // const numSegments = Math.ceil(totalHeight / height);
              // const segmentHeight = height;

              for (let i = 0; i < numSegments; i++) {
                const barHeight = Math.min(
                  segmentHeight,
                  totalHeight - i * segmentHeight
                );
                d3.select(this)
                  .append("rect")
                  .attr("class", "out-bar-segment")
                  .attr("x", sankey.nodeWidth() / 2 + i * sankey.nodeWidth())
                  .attr("y", (d) => {
                    const baseHeight = d.y1 - d.y0;
                    return -1 * barHeight + baseHeight;
                  })
                  .attr("height", barHeight)
                  .attr("width", sankey.nodeWidth() / 2)
                  .attr("fill", d3.color(targetColors.get(d.name)).darker(0.5))
                  .attr("opacity", 0.7);
              }
            });

          node
            .append("text")
            .attr("x", (d) => (d.x0 < width / 2 ? 6 + sankey.nodeWidth() : -6))
            .attr("y", (d) => (d.y1 - d.y0) / 2)
            .attr("dy", "0.35em")
            .attr("text-anchor", (d) => (d.x0 < width / 2 ? "start" : "end"))
            .text((d) => {
              if (isFinalNode(d)) {
                return `${d.name}`;
              } else {
                return `${d.name}`;
              }
            });

          node
            .append("title")
            .text(
              (d) =>
                `${d.source_name}\n` +
                `Description: ${d.source_description || "No description available"}\n` +
                `Companies: ${d.source_companies || "No companies available"}\n` +
                `Source Org: ${d.source_org}\n` +
                `Total Funding In: $${d.fundingIn.toFixed(2)}M\n` +
                `Total Funding Out: $${d.fundingOut.toFixed(2)}M\n` +
                `Target Funding: $${d.targetFunding.toFixed(2)}M`
            );

            link
              .append("title")
              .text((d) =>
                `${d.source.source_name} → ${d.target.name}\n` +
                `Source Org: ${d.source.source_org}\n` +         
                `Value: $${d.value.toFixed(2)}M\n` +
                `Companies: ${d.source_companies || "No companies available"}\n` +
                `Description: ${d.source_description || "No description available"}`
              );

          // The function for moving the nodes
          function dragmove(event, d) {
            d.y0 = Math.max(0, Math.min(height - (d.y1 - d.y0), event.y));
            d.y1 = d.y0 + (d.y1 - d.y0);
            d3.select(this).attr(
              "transform",
              "translate(" + d.x0 + "," + d.y0 + ")"
            );
            sankey.update(graph);
            link.attr("d", d3.sankeyLinkHorizontal());
          }

          // Calculate the scaling factor
          const maxNodeValue = d3.max(graph.nodes, (d) => d.value);
          const maxNodeHeight = d3.max(graph.nodes, (d) => d.y1 - d.y0);
          const scalingFactor = maxNodeHeight / maxNodeValue;

          // Calculate the height of a node with a value of 100M
          const legendHeight = 100 * scalingFactor;

          // Add legend
          const legend = container
            .append("svg")
            .attr("width", 200)
            .attr("height", legendHeight + 40) // Adjust height to fit the legend
            .attr("class", "legend");

          const legendGroup = legend.append("g");
          const legendWidth = sankey.nodeWidth();

          legendGroup
            .append("rect")
            .attr("width", legendWidth)
            .attr("height", legendHeight)
            .attr("fill", "#ccc")
            .attr("opacity", 0.7);

          legendGroup
            .append("text")
            .attr("x", legendWidth + 10)
            .attr("y", legendHeight / 2)
            .attr("dy", "0.35em")
            .text("100M Value");
        });
      })
      .catch(function (error) {
        console.error("Error loading data:", error);
        document.getElementById("error").textContent =
          "Error loading data: " + error.message;
      });
  } catch (error) {
    console.error("Error:", error);
    document.getElementById("error").textContent = "Error: " + error.message;
  }
});
