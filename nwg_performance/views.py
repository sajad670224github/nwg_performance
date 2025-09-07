
from django.contrib.auth.views import LoginView

class CustomLoginView(LoginView):
    template_name = 'registration/login.html'
    redirect_authenticated_user = True

class HomeView(LoginView):
    template_name = 'home.html'
   # redirect_authenticated_user = True

class SlaKpiView(LoginView):
    template_name = 'sla_template.html'
   # redirect_authenticated_user = True