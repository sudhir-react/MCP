from fastmcp import FastMCP 

mcp = FastMCP(name = "Restaurant")

@mcp.tool
def menu():
    """The Food items provided by the restaurent"""
    food = ['biriyani', 'noodles', 'soup', 'juices', ]
    return food

@mcp.tool
def location():
    """The Location where restaurent are available"""
    location = ['Hyderabad', 'Delhi', 'Mumbai', 'Chennai']
    return location

if __name__ == "__main__":
    mcp.run(transport='streamable-http',port = 5001,host = '0.0.0.0')

