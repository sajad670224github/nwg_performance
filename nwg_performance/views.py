
from django.contrib.auth.views import LoginView
from django.shortcuts import render
from django.shortcuts import redirect
from django.views import View
from django_redis import get_redis_connection

class CustomLoginView(LoginView):
    template_name = 'registration/login.html'
    redirect_authenticated_user = True

class HomeView(LoginView):
    template_name = 'home.html'
   # redirect_authenticated_user = True

class SlaKpiView(LoginView):
    template_name = 'sla_template.html'
   # redirect_authenticated_user = True

def logout_request(request):
    # Clear session
    user_id = request.session.get('user_id')
    conn = get_redis_connection('default')
    redis_key = f"user_auth:{user_id}"
    conn.delete(redis_key)
    request.session.flush()

    redirect_url = (
        "http://127.0.0.1:8004/accounts/login/"
    )
    return redirect(redirect_url)


def get_auth_data(user_id):
    conn = get_redis_connection('default')
    raw  = conn.hgetall(f"user_auth:{user_id}")
    return {k.decode(): v.decode() for k, v in raw.items()}

class IntraServiceLoginView(View):
    def get(self, request):
        token = request.GET.get('token')
        session_key = request.GET.get('session')
        user_id = request.GET.get('user_id')
        request.session['user_id'] = user_id
        is_user_authenticate = True
        # TO DO
        # Check weather user is authenticated or not
        user_data = get_auth_data(user_id)


        print(f"{user_id}\n{session_key}\n{token}")
        print('-'*100)
        print(user_data)
        return render(request, 'sla_template.html', {
            'user_id': user_id,
            'token':    token,
        })